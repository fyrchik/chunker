package chunker

import "io"

// Chunk is a generic chunk structure.
type Chunk struct {
	// Cut is a checksum of a chunk.
	Cut uint64
	// Data contains chunk's contents.
	Data []byte
}

// Chunker is a generic interface which can split stream of bytes
// into chunks.
type Chunker interface {
	// Reset initializes Chunker to use provided reader.
	Reset(r io.Reader)
	// Next returns next Chunk. buf is a preallocated buffer for chunk's contents.
	// Any particular implementation should not assume that buf != nil.
	// Next should return nil error iff Chunk is not nil.
	Next(buf []byte) (*Chunk, error)
}

const (
	KiB = 1024
	MiB = 1024 * 1024
)
