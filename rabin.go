package chunker

import (
	"math/bits"
)

const (
	defaultPoly = Poly(0x3DA3358B4DC173)
	minSize     = 1 << 19
	maxSize     = 1 << 23
	avgSize     = 1 << 20
	mask        = avgSize - 1
	winSize     = 64
)

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
	data   []byte // TOOD replace with io.Reader

	pos   int
	min   int
	max   int
	start int

	digest Poly
}

// Reset implements Chunker interface.
func (r *rabin) Reset(br []byte) {
	r.data = br
	r.digest = 0
	r.pos = 0
	r.start = 0
	r.slide(1)
}

func NewRabin() *rabin {
	r := &rabin{
		min: minSize,
		max: maxSize,
	}

	return r
}

func (r *rabin) Next(buf []byte) (*Chunk, error) {
	buf = buf[:0]

	r.start = r.pos
	end := min(r.start+minSize, len(r.data))

	for ; r.pos < end; r.pos++ {
		r.slide(r.data[r.pos])
	}

	if r.digest&mask == 0 || end == len(r.data) {
		return &Chunk{
			Length: r.pos - r.start,
			Digest: uint64(r.digest),
			Data:   append(buf, r.data[r.start:r.pos]...),
		}, nil
	}

	end = min(r.start+maxSize, len(r.data))
	for ; r.pos < end; r.pos++ {
		r.slide(r.data[r.pos])

		if r.digest&mask == 0 {
			r.pos++

			return &Chunk{
				Length: r.pos - r.start,
				Digest: uint64(r.digest),
				Data:   append(buf, r.data[r.start:r.pos]...),
			}, nil
		}
	}

	r.pos = len(r.data)

	return &Chunk{
		Length: r.pos - r.start,
		Digest: uint64(r.digest),
		Data:   append(buf, r.data[r.start:]...),
	}, nil
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

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
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
