package sntable

import (
	"bytes"
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/golang/snappy"
)

// Reader instances can seek and iterate across data in tables.
type Reader struct {
	r io.ReaderAt

	index     []blockInfo
	maxOffset int64
}

// NewReader opens a reader.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	tmp := make([]byte, 16+binary.MaxVarintLen64)

	// read footer
	footerOffset := size - 16
	if _, err := r.ReadAt(tmp[:16], footerOffset); err != nil {
		return nil, err
	}

	// parse footer
	if !bytes.Equal(tmp[8:16], magic) {
		return nil, errBadMagic
	}
	indexOffset := int64(binary.LittleEndian.Uint64(tmp[:8]))

	// read index
	var index []blockInfo
	var info blockInfo

	for pos := indexOffset; pos < footerOffset; {
		tmp = tmp[:2*binary.MaxVarintLen64]
		if x := footerOffset - pos; x < int64(len(tmp)) {
			tmp = tmp[:int(x)]
		}

		_, err := r.ReadAt(tmp, pos)
		if err != nil {
			return nil, err
		}

		u1, n := binary.Uvarint(tmp[0:])
		pos += int64(n)

		u2, n := binary.Uvarint(tmp[n:])
		pos += int64(n)

		info.MaxKey += u1
		info.Offset += int64(u2)
		index = append(index, info)
	}

	return &Reader{
		r: r,

		index:     index, // block offsets
		maxOffset: indexOffset,
	}, nil
}

// NumBlocks returns the number of stored blocks.
func (r *Reader) NumBlocks() int {
	return len(r.index)
}

// Append retrieves a single value for a key. Unlike Get it doesn't
// appends it to dst instead of allocating a new byte slice.
// It may return an ErrNotFound error.
func (r *Reader) Append(dst []byte, key uint64) ([]byte, error) {
	iter, err := r.Seek(key)
	if err != nil {
		return dst, err
	}
	defer iter.Release()

	if !iter.Next() || iter.Key() != key {
		return dst, ErrNotFound
	}
	return append(dst, iter.Value()...), nil
}

// Get is a shortcut for Append(nil, key).
// It may return an ErrNotFound error.
func (r *Reader) Get(key uint64) ([]byte, error) {
	return r.Append(nil, key)
}

// Seek returns an iterator starting at the position >= key.
func (r *Reader) Seek(key uint64) (*Iterator, error) {
	b, err := r.SeekBlock(key)
	if err != nil {
		return nil, err
	}

	s := b.SeekSection(key)
	s.Seek(key)
	return &Iterator{r: r, b: b, s: s}, nil
}

// GetBlock returns a reader for the n-th block.
func (r *Reader) GetBlock(bpos int) (*BlockReader, error) {
	if len(r.index) == 0 {
		return &BlockReader{}, nil
	}
	if bpos < 0 {
		bpos = 0
	}
	if bpos >= len(r.index) {
		return &BlockReader{
			bpos: len(r.index),
		}, nil
	}
	return r.readBlock(bpos)
}

// SeekBlock seeks the block containing the key.
func (r *Reader) SeekBlock(key uint64) (*BlockReader, error) {
	bpos := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxKey >= key
	})
	return r.GetBlock(bpos)
}

func (r *Reader) readBlock(bpos int) (*BlockReader, error) {
	min := r.index[bpos].Offset
	max := r.maxOffset
	if next := bpos + 1; next < len(r.index) {
		max = r.index[next].Offset
	}

	raw := fetchBuffer(int(max - min))
	if _, err := r.r.ReadAt(raw, min); err != nil {
		releaseBuffer(raw)
		return nil, err
	}

	var block []byte
	switch cBitPos := len(raw) - 1; raw[cBitPos] {
	case blockNoCompression:
		block = raw[:cBitPos]
	case blockSnappyCompression:
		defer releaseBuffer(raw)

		sz, err := snappy.DecodedLen(raw[:cBitPos])
		if err != nil {
			return nil, err
		}

		plain := fetchBuffer(sz)
		if block, err = snappy.Decode(plain, raw[:cBitPos]); err != nil {
			releaseBuffer(plain)
			return nil, err
		}
	default:
		releaseBuffer(raw)
		return nil, errBadCompression
	}

	return &BlockReader{
		block:  block,
		bpos:   bpos,
		scnt:   int(binary.LittleEndian.Uint32(block[len(block)-4:])),
		maxKey: r.index[bpos].MaxKey,
	}, nil
}

// --------------------------------------------------------------------

// BlockReader reads a single block.
type BlockReader struct {
	block  []byte
	bpos   int // the current block position
	scnt   int // the section count
	maxKey uint64
}

// NumSections returns the number of sections in this block.
func (r *BlockReader) NumSections() int { return r.scnt }

// Pos returns the index position the current block within the table.
func (r *BlockReader) Pos() int { return r.bpos }

// GetSection gets a single section.
func (r *BlockReader) GetSection(spos int) *SectionReader {
	if spos < 0 {
		spos = 0
	}
	if spos >= r.scnt {
		return &SectionReader{spos: r.scnt}
	}

	min := r.sectionOffset(spos)
	max := r.sectionOffset(spos + 1)
	return &SectionReader{section: r.block[min:max], spos: spos}
}

// SeekSection seeks the section for a key.
func (r *BlockReader) SeekSection(key uint64) *SectionReader {
	if key > r.maxKey {
		return r.GetSection(r.scnt)
	}

	spos := sort.Search(r.scnt, func(i int) bool {
		off := r.sectionOffset(i)
		first, _ := binary.Uvarint(r.block[off:]) // first key of the section
		return first > key
	}) - 1
	return r.GetSection(spos)
}

// Release releases the block reader and frees up resources. The reader must not be used
// after this method is called.
func (r *BlockReader) Release() { bufPool.Put(r.block) }

// The starting offset of the section within the block.
func (r *BlockReader) sectionOffset(spos int) int {
	if spos < 1 {
		return 0
	} else if spos >= r.scnt {
		return len(r.block) - r.scnt*4
	} else {
		nn := len(r.block) - r.scnt*4 + (spos-1)*4
		return int(binary.LittleEndian.Uint32(r.block[nn:]))
	}
}

// SectionReader reads an individual section within a block.
type SectionReader struct {
	section []byte

	spos int // the section
	read int // bytes read

	key uint64 // current key
	val []byte // current value
}

// Seek positions the cursor before the key.
func (r *SectionReader) Seek(key uint64) bool {
	for r.More() {
		inc, n := binary.Uvarint(r.section[r.read:])
		r.read += n
		r.key += inc
		if r.key >= key {
			r.read -= n
			r.key -= inc
			return true
		}

		if r.More() {
			vln, n := binary.Uvarint(r.section[r.read:])
			r.read += n
			r.val = r.section[r.read : r.read+int(vln)]
			r.read += int(vln)
		}
	}
	return false
}

// Pos returns the index position the current section within the block.
func (r *SectionReader) Pos() int { return r.spos }

// Key returns the key if the current entry.
func (r *SectionReader) Key() uint64 { return r.key }

// Value returns the value of the current entry. Please note that values
// are temporary buffers and must be copied if used beyond the next cursor move.
func (r *SectionReader) Value() []byte { return r.val }

// More returns true if more data can be read in the section.
func (r *SectionReader) More() bool { return r.read < len(r.section) }

// Next advances the cursor to the next entry within the section and
// returns true if successful.
func (r *SectionReader) Next() bool {
	if r.More() {
		inc, n := binary.Uvarint(r.section[r.read:])
		r.read += n
		r.key += inc
	}

	if r.More() {
		vln, n := binary.Uvarint(r.section[r.read:])
		r.read += n
		r.val = r.section[r.read : r.read+int(vln)]
		r.read += int(vln)
		return true
	}

	return false
}

// --------------------------------------------------------------------

// Iterator is a convenience wrapper around BlockReader and SectionReader
// which can (forward-) iterate over keys across block and section boundaries.
type Iterator struct {
	r *Reader
	b *BlockReader
	s *SectionReader

	err error
}

// Key returns the key if the current entry.
func (i *Iterator) Key() uint64 { return i.s.Key() }

// Value returns the value of the current entry. Please note that values
// are temporary buffers and must be copied if used beyond the next cursor move.
func (i *Iterator) Value() []byte { return i.s.Value() }

// More returns true if more data can be read.
func (i *Iterator) More() bool {
	if i.err != nil {
		return false
	}

	return i.s.More() || i.s.Pos()+1 < i.b.NumSections() || i.b.Pos()+1 < i.r.NumBlocks()
}

// Next advances the cursor to the next entry and returns true if successful.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}

	// more entries in the section
	if i.s.More() {
		return i.s.Next()
	}

	// more sections in the block
	if n := i.s.Pos() + 1; n < i.b.NumSections() {
		i.s = i.b.GetSection(n)
		return i.s.Next()
	}

	// more blocks
	if n := i.b.Pos() + 1; n < i.r.NumBlocks() {
		i.b, i.err = i.r.GetBlock(n)
		i.s = i.b.GetSection(0)
		return i.s.Next()
	}

	return false
}

// Err exposes iterator errors, if any.
func (i *Iterator) Err() error {
	return i.err
}

// Release releases the iterator and frees up resources. The iterator must not be used
// after this method is called.
func (i *Iterator) Release() {
	i.b.Release()
	i.err = errReleased
}

// --------------------------------------------------------------------

var bufPool sync.Pool

func fetchBuffer(sz int) []byte {
	if v := bufPool.Get(); v != nil {
		if p := v.([]byte); sz <= cap(p) {
			return p[:sz]
		}
	}
	return make([]byte, sz)
}

func releaseBuffer(p []byte) {
	if cap(p) != 0 {
		bufPool.Put(p)
	}
}
