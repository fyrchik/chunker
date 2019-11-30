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

func calcTables(poly Poly, winSize int) {
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
}

func init() {
	calcTables(defaultPoly, winSize)
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
		r.updateBuf()
		if r.err != nil && r.err != io.EOF || r.end == 0 {
			return nil, r.err
		}
	}

	r.start = r.bpos
	buf = buf[:0]
	count := 1

	for ; count <= r.min; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.bpos == r.end && count < r.min-1 {
			if r.err == io.EOF {
				break
			}

			buf = append(buf, r.buf[r.start:r.bpos]...)

			r.updateBuf()
			if r.err != nil && r.err != io.EOF {
				return nil, r.err
			}
		}
	}

	if r.digest&mask == 0 || (r.bpos == r.end && r.err == io.EOF) {
		return &Chunk{
			Digest: uint64(r.digest),
			Data:   append(buf, r.buf[r.start:r.bpos]...),
		}, nil
	}

	for ; count <= r.max; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.digest&mask == 0 {
			return &Chunk{
				Digest: uint64(r.digest),
				Data:   append(buf, r.buf[r.start:r.bpos]...),
			}, nil
		} else if r.bpos == r.end {
			buf = append(buf, r.buf[r.start:r.bpos]...)

			r.updateBuf()
			if r.err == io.EOF {
				break
			} else if r.err != nil {
				return nil, r.err
			}
		}
	}

	return &Chunk{
		Digest: uint64(r.digest),
		Data:   append(buf, r.buf[r.start:r.bpos]...),
	}, nil
}

func (r *rabin) updateBuf() {
	r.bpos = 0
	r.end = 0
	r.start = 0

	if r.err != nil {
		return
	}
	r.end, r.err = io.ReadFull(r.r, r.buf[:])

	if r.err == io.ErrUnexpectedEOF {
		r.err = io.EOF
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
