package sntable

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// WriterOptions define writer specific options.
type WriterOptions struct {
	// BlockSize is the minimum uncompressed size in bytes of each table block.
	// Default: 4KiB.
	BlockSize int

	// BlockRestartInterval is the number of keys between restart points
	// for delta encoding of keys.
	//
	// Default: 16.
	BlockRestartInterval int

	// The compression codec to use.
	// Default: SnappyCompression.
	Compression Compression
}

func (o *WriterOptions) norm() *WriterOptions {
	var oo WriterOptions
	if o != nil {
		oo = *o
	}

	if oo.BlockSize < 1 {
		oo.BlockSize = 1 << 12
	}
	if oo.BlockRestartInterval < 1 {
		oo.BlockRestartInterval = 16
	}
	if !oo.Compression.isValid() {
		oo.Compression = SnappyCompression
	}

	return &oo
}

// Writer instances can write a table.
type Writer struct {
	w io.Writer
	o *WriterOptions

	block blockInfo // the current block info
	blen  int       // the number of entries in the current block
	soffs []int     // section offsets in the current block

	buf []byte // plain buffer
	snp []byte // snappy  buffer
	tmp []byte // scratch buffer

	index []blockInfo
}

// NewWriter wraps a writer and returns a Writer.
func NewWriter(w io.Writer, o *WriterOptions) *Writer {
	return &Writer{
		w:   w,
		o:   o.norm(),
		tmp: make([]byte, 2*binary.MaxVarintLen64),
	}
}

// Append appends a cell to the store.
func (w *Writer) Append(key uint64, value []byte) error {
	if w.tmp == nil {
		return errClosed
	}

	if key <= w.block.MaxKey && (w.blen != 0 || len(w.index) != 0) {
		return fmt.Errorf("sntable: attempted an out-of-order append, %v must be > %v", key, w.block.MaxKey)
	}

	if len(w.buf) != 0 && len(w.buf)+len(value)+2*binary.MaxVarintLen64 > w.o.BlockSize {
		if err := w.flush(); err != nil {
			return err
		}
	}

	skey := key
	if w.blen%w.o.BlockRestartInterval == 0 { // new section?
		w.soffs = append(w.soffs, len(w.buf))
	} else {
		skey -= w.block.MaxKey // apply delta-encoding
	}

	n := binary.PutUvarint(w.tmp[0:], uint64(skey))
	n += binary.PutUvarint(w.tmp[n:], uint64(len(value)))
	w.buf = append(w.buf, w.tmp[:n]...)
	w.buf = append(w.buf, value...)

	w.blen++
	w.block.MaxKey = key

	return nil
}

// Close closes the writer
func (w *Writer) Close() error {
	if w.tmp == nil {
		return errClosed
	}
	if err := w.flush(); err != nil {
		return err
	}

	indexOffset := w.block.Offset
	if err := w.writeIndex(); err != nil {
		return err
	}

	if err := w.writeFooter(indexOffset); err != nil {
		return err
	}
	w.tmp = nil
	return nil
}

func (w *Writer) writeIndex() error {
	var prev blockInfo

	for i, ent := range w.index {
		key := ent.MaxKey
		off := ent.Offset
		if i != 0 { // delta-encode
			key -= prev.MaxKey
			off -= prev.Offset
		}
		prev = ent

		n := binary.PutUvarint(w.tmp[0:], uint64(key))
		n += binary.PutUvarint(w.tmp[n:], uint64(off))

		if err := w.writeRaw(w.tmp[:n]); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeFooter(indexOffset int64) error {
	binary.LittleEndian.PutUint64(w.tmp[0:], uint64(indexOffset))
	if err := w.writeRaw(w.tmp[:8]); err != nil {
		return err
	}
	if err := w.writeRaw(magic); err != nil {
		return err
	}
	return nil
}

func (w *Writer) writeRaw(p []byte) error {
	n, err := w.w.Write(p)
	w.block.Offset += int64(n)
	return err
}

func (w *Writer) flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	for _, o := range w.soffs {
		if o > 0 {
			binary.LittleEndian.PutUint32(w.tmp, uint32(o))
			w.buf = append(w.buf, w.tmp[:4]...)
		}
	}
	binary.LittleEndian.PutUint32(w.tmp, uint32(len(w.soffs)))
	w.buf = append(w.buf, w.tmp[:4]...)

	var block []byte
	switch w.o.Compression {
	case SnappyCompression:
		w.snp = snappy.Encode(w.snp[:cap(w.snp)], w.buf)
		if len(w.snp) < len(w.buf)-len(w.buf)/4 {
			block = append(w.snp, blockSnappyCompression)
		} else {
			block = append(w.buf, blockNoCompression)
		}
	default:
		block = append(w.buf, blockNoCompression)
	}

	w.index = append(w.index, w.block)
	w.buf = w.buf[:0]
	w.soffs = w.soffs[:0]
	w.blen = 0

	return w.writeRaw(block)
}
