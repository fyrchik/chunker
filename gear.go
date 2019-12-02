package chunker

import (
	"io"
	"math/rand"
)

type gear struct {
	r      io.Reader
	digest uint32
	err    error
	buf    [bufSize]byte
	bpos   int
	start  int
	end    int
	table  [256]uint32
}

func newGearTable(r *rand.Rand) (buf [256]uint32) {
	for i := 0; i < 256; i++ {
		buf[i] = r.Uint32()
	}

	return
}

// Next implements Chunker interface.
func (g *gear) Next(buf []byte) (*Chunk, error) {
	if g.end == 0 || g.bpos == g.end {
		if !g.updateBuf() {
			return nil, g.err
		}
	}

	g.start = g.bpos
	buf = buf[:0]

	for {
		g.slide(g.buf[g.bpos])
		g.bpos++

		if g.digest&mask == 0 {
			break
		}

		if g.bpos == g.end {
			buf = append(buf, g.buf[g.start:g.bpos]...)

			if !g.updateBuf() {
				if g.err == io.EOF {
					return g.chunk(buf), nil
				} else if g.err != nil {
					return nil, g.err
				}
			}
		}
	}

	buf = append(buf, g.buf[g.start:g.bpos]...)

	return g.chunk(buf), nil
}

func (g *gear) chunk(buf []byte) *Chunk {
	return &Chunk{
		Digest: uint64(g.digest),
		Data:   buf,
	}
}

// Reset implements Chunker interface.
func (g *gear) Reset(r io.Reader) {
	g.r = r
	g.digest = 0
	g.bpos = 0
	g.start = 0
	g.end = 0
	g.slide(1)
}

func (g *gear) updateBuf() bool {
	g.bpos = 0
	g.end = 0
	g.start = 0

	if g.err != nil {
		return false
	}

	g.end, g.err = io.ReadFull(g.r, g.buf[:])

	if g.err == io.ErrUnexpectedEOF {
		g.err = io.EOF
	}

	return g.end != 0
}

func (g *gear) slide(b byte) {
	g.digest <<= 1
	g.digest += g.table[b]
}
