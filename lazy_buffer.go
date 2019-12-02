package chunker

import "io"

// LazyBuf is a generic struct which reads data from provided reader
// in chunks storing current result in buffer.
type LazyBuf struct {
	io.Reader
	Buf [bufSize]byte
	Pos int
	end int
	err error

	onUpdate func(*LazyBuf)
}

func (b *LazyBuf) Next() (x byte) {
	if b.Pos == b.end {
		if b.err != nil {
			return
		} else if b.onUpdate != nil {
			b.onUpdate(b)
		}

		if !b.Update() {
			return
		}
	}

	x = b.Buf[b.Pos]
	b.Pos++

	return
}

func (b *LazyBuf) Update() bool {
	b.Pos = 0
	b.end = 0

	if b.err != nil {
		return false
	}

	b.end, b.err = io.ReadFull(b.Reader, b.Buf[:])

	if b.err == io.ErrUnexpectedEOF {
		b.err = io.EOF
	}

	return b.end != 0
}
