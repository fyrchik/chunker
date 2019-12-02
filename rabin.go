package chunker

import (
	"io"
	"math/bits"
)

const defaultPoly = Poly(0x3DA3358B4DC173)

const (
	// minSize is default min chunk size = 512 KiB.
	minSize = 1 << 19
	// maxSize is default max chunk size = 8 MiB.
	maxSize = 1 << 23
	// avgSize is an average chunk size = 1 MiB.
	avgSize = 1 << 20
	// mask is used for detecting boundaries and is calculated based on
	// average chunk size.
	mask = avgSize - 1
	// winSize is default window size
	winSize = 64
	// bufSize is default internal buffer size
	bufSize = 2 * MiB
)

// Poly represents polynomial over GF(2).
type Poly uint64

var (
	outTable [256]Poly
	modTable [256]Poly
)

// calcTables returns 2 tables to speed up calculations of rabin fingerprint.
// outTable is used for fast remove of the first byte from the sliding window:
//  Let's assume our data is [b0, b1, ... bN] b ... with sliding window in brackets.
//  Define Poly(1001..) as a polynomial over GF(2) = 1 + 0*x + 0*x^2 + 1*x^3 ...
//  H(b0,..bN) is defined over bytes and is equal to Poly(bits of b0, bits of b1...).
//  Note: Poly's coefficients are individual bits and sliding in H is performed byte-by-byte.
//  Then H(b1..bN+1) = H(b0..bN) * x^8 + bN+1 with upper 8 bits removed =
//  = H(b0..bN)*x^8 + bN+1 + H(b0,0,0,0...). The last H is our outTable[b0]
// modTable is used to replaced modular division with a simple XOR.
//  It contains precalculated reductions of all polynomials which exceed the
//  degree of remainder polynomial by no more than 8.
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

	start int
	ch    []byte
	lb    LazyBuf

	min int
	max int

	digest Poly
}

// Reset implements Chunker interface.
func (r *rabin) Reset(br io.Reader) {
	r.lb = LazyBuf{
		Reader: br,
		onUpdate: func(b *LazyBuf) {
			r.ch = append(r.ch, b.Buf[r.start:]...)
			r.start = 0
		},
	}
	r.lb.Update()

	r.digest = 0
	r.slide(1)
}

// NewRabinWithParams returns rabin Chunker with specified
// min and max chunks sizes.
func NewRabinWithParams(min, max int) *rabin {
	return &rabin{
		min: min,
		max: max,
	}
}

// NewRabin returns default rabin Chunker.
func NewRabin() *rabin {
	return NewRabinWithParams(minSize, maxSize)
}

// Next implements Chunker interface.
func (r *rabin) Next(buf []byte) (*Chunk, error) {
	r.start = r.lb.Pos
	r.ch = buf[:0]

	count := 1
	for ; count <= r.min; count++ {
		b := r.lb.Next()
		if r.lb.err != nil {
			if r.lb.err == io.EOF {
				if len(r.ch) == 0 && count == 1 {
					return nil, r.lb.err
				}

				return r.chunk(), nil
			}

			return nil, r.lb.err
		}

		r.slide(b)
	}

	if r.digest&mask == 0 {
		return r.chunk(), nil
	}

	for ; count <= r.max; count++ {
		b := r.lb.Next()
		if r.lb.err != nil {
			if r.lb.err == io.EOF {
				return r.chunk(), nil
			}

			return nil, r.lb.err
		}

		r.slide(b)

		if r.digest&mask == 0 {
			return r.chunk(), nil
		}
	}

	return r.chunk(), nil
}

func (r *rabin) chunk() *Chunk {
	return &Chunk{
		Digest: uint64(r.digest),
		Data:   append(r.ch, r.lb.Buf[r.start:r.lb.Pos]...),
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
