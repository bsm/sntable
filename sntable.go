package sntable

import "errors"

var magic = []byte{71, 39, 134, 190, 31, 122, 101, 219}

const (
	blockNoCompression     = 0
	blockSnappyCompression = 1
)

// ErrNotFound is returned by the reader when a key cannot be found.
var ErrNotFound = errors.New("sntable: not found")

var (
	errClosed         = errors.New("sntable: is closed")
	errBadMagic       = errors.New("sntable: bad magic byte sequence")
	errBadCompression = errors.New("sntable: bad compression codec")
	errReleased       = errors.New("sntable: iterator was released")
)

type blockInfo struct {
	MaxKey uint64 // maximum key in the block
	Offset int64  // block offset position
}

// --------------------------------------------------------------------

// Compression is the compression codec
type Compression byte

func (c Compression) isValid() bool {
	return c >= SnappyCompression && c <= unknownCompression
}

// Supported compression codecs
const (
	SnappyCompression Compression = iota
	NoCompression
	unknownCompression
)
