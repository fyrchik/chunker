package chunker

import (
	"io"
	"math/bits"
)

const defaultPoly = Poly(0x3DA3358B4DC173)

const (
	minSize = 1 << 19
	maxSize = 1 << 23
	avgSize = 1 << 20
	mask    = avgSize - 1
	winSize = 64
	bufSize = 2 * MiB
)

// Poly represents polynomial over GF(2).
type Poly uint64

var (
	outTable [256]Poly
	modTable [256]Poly
)

func calcTables(poly Poly, winSize int) (outTable [256]Poly, modTable [256]Poly) {
	for b := 0; b < 256; b++ {
		var h Poly

		h = appendByte(h, byte(b), poly)

		for i := 0; i < winSize-1; i++ {
			h = appendByte(h, 0, poly)
		}

		outTable[b] = h
	}

	k := deg(poly)

	for b := 0; b < 256; b++ {
		p := Poly(b) << k
		modTable[b] = mod(p, poly) | p
	}

	return outTable, modTable
}

func init() {
	outTable, modTable = calcTables(defaultPoly, winSize)
}

type rabin struct {
	window [winSize]byte
	wpos   int
	r      io.Reader
	err    error

	bpos  int
	start int
	end   int
	buf   [bufSize]byte

	pos int
	min int
	max int

	digest Poly
}

// Reset implements Chunker interface.
func (r *rabin) Reset(br io.Reader) {
	r.r = br
	r.digest = 0
	r.pos = 0
	r.bpos = 0
	r.end = 0
	r.slide(1)
}

func NewRabinWithParams(min, max int) *rabin {
	return &rabin{
		min: min,
		max: max,
	}
}

func NewRabin() *rabin {
	return NewRabinWithParams(minSize, maxSize)
}

func (r *rabin) Next(buf []byte) (*Chunk, error) {
	if r.end == 0 || r.bpos == r.end {
		if !r.updateBuf() {
			return nil, r.err
		}
	}

	r.start = r.bpos
	buf = buf[:0]
	count := 1

	for ; count <= r.min; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		// if chunk is still less than minimal size
		// but more data needs to be read
		if r.bpos == r.end {
			buf = append(buf, r.buf[r.start:r.bpos]...)

			if count < r.min-1 && !r.updateBuf() {
				if r.err != io.EOF {
					return nil, r.err
				}

				return r.chunk(buf), nil
			}
		}
	}

	if r.digest&mask == 0 || (r.bpos == r.end && !r.updateBuf() && r.err == io.EOF) {
		return r.chunk(buf), nil
	}

	for ; count <= r.max; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.digest&mask == 0 {
			return r.chunk(append(buf, r.buf[r.start:r.bpos]...)), nil
		} else if r.bpos == r.end {
			buf = append(buf, r.buf[r.start:r.bpos]...)
			r.start = r.bpos

			if !r.updateBuf() {
				if r.err == io.EOF {
					break
				}

				return nil, r.err
			}
		}
	}

	return r.chunk(append(buf, r.buf[r.start:r.bpos]...)), nil
}

// updateBuf reads more data from buffer and returns
// true is some data was read.
func (r *rabin) updateBuf() bool {
	r.bpos = 0
	r.end = 0
	r.start = 0

	if r.err != nil {
		return false
	}

	r.end, r.err = io.ReadFull(r.r, r.buf[:])

	if r.err == io.ErrUnexpectedEOF {
		r.err = io.EOF
	}

	return r.end != 0
}

func (r *rabin) chunk(buf []byte) *Chunk {
	return &Chunk{
		Digest: uint64(r.digest),
		Data:   buf,
	}
}

func (r *rabin) append(b byte) {
	index := r.digest >> (deg(defaultPoly) - 8)
	r.digest <<= 8
	r.digest |= Poly(b)
	r.digest ^= modTable[index]
}

func (r *rabin) slide(b byte) {
	out := r.window[r.wpos]
	r.window[r.wpos] = b
	r.digest ^= outTable[out]
	r.wpos = (r.wpos + 1) % winSize
	r.append(b)
}

// deg returns degree of a polynomial p
// and -1 if p == 0
func deg(p Poly) int {
	return bits.Len64(uint64(p)) - 1
}

func mod(p, q Poly) Poly {
	dq := deg(q)
	for dp := deg(p); dp >= dq; dp = deg(p) {
		p ^= q << (dp - dq)
	}

	return p
}

func appendByte(sum Poly, b byte, poly Poly) Poly {
	sum <<= 8
	sum |= Poly(b)

	return mod(sum, poly)
}
