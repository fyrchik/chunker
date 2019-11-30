package chunker

import (
	"bytes"
	"errors"
	"io"
)

// gentleReader is a wrapper over reader which sets Used
// if an underlying reader was used after returning an error.
// It is used in tests to check that all chunkers are well-behaved
// meaning they do not use provided reader after any error.
type gentleReader struct {
	io.Reader
	Err  error
	Used bool
}

type errorReader struct {
	io.Reader
	index int
	after int
}

func newGentleReaderFromBuf(buf []byte) *gentleReader {
	return &gentleReader{
		Reader: bytes.NewReader(buf),
	}
}

// Read implements io.Reader interface.
func (r *gentleReader) Read(p []byte) (n int, err error) {
	if r.Err != nil {
		r.Used = true
	}

	n, r.Err = r.Reader.Read(p)

	return n, r.Err
}

func newErrorReaderFromBuf(after int, buf []byte) *errorReader {
	return &errorReader{
		after:  after,
		Reader: bytes.NewReader(buf),
	}
}

// Read implements io.Reader interface.
func (r *errorReader) Read(p []byte) (n int, err error) {
	if r.index == r.after {
		return 0, errors.New("error on read")
	}

	if r.index+len(p) > r.after {
		p = p[:r.after-r.index]
	}

	n, err = r.Reader.Read(p)
	r.index += n

	return
}
