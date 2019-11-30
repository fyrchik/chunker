package chunker

import "io"

// Chunk is a generic chunk structure.
type Chunk struct {
	// Length is the length of the chunk.
	Length int
	// Digest is a checksum of a chunk.
	Digest uint64
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
	Next(buf []byte) (*Chunk, error)
}

const (
	KiB = 1024
	MiB = 1024 * 1024
)
