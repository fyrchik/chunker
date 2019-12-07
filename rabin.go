package chunker

import (
	"io"
	"math/bits"
)

const defaultPoly = Poly(0x3DA3358B4DC173)

const (
	// minSize is default min chunk size.
	minSize = 512 * KiB
	// maxSize is default max chunk size.
	maxSize = 8 * MiB
	// avgSize is an average chunk size.
	avgSize = 1 * MiB
	// mask is used for detecting boundaries and is calculated based on
	// average chunk size.
	mask = avgSize - 1
	// winSize is default window size
	winSize = 64
	// bufSize is default internal buffer size
	bufSize = 512 * KiB
)

// Poly represents polynomial over GF(2).
type Poly uint64

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

type rabin struct {
	window [winSize]byte
	wpos   int
	r      io.Reader
	err    error

	bpos  int
	start int
	end   int
	buf   [bufSize]byte

	poly  Poly
	shift int
	out   [256]Poly
	mod   [256]Poly

	lastChunk []byte

	min int
	max int

	digest Poly
}

// Reset implements Chunker interface.
func (r *rabin) Reset(br io.Reader) {
	r.r = br
	r.digest = 0
	r.bpos = 0
	r.end = 0
	r.slide(1)
}

// NewRabinWithParams returns rabin Chunker with specified
// min and max chunks sizes.
func NewRabinWithParams(min, max int) *rabin {
	r := &rabin{
		min:   min,
		max:   max,
		poly:  defaultPoly,
		shift: deg(defaultPoly) - 8,
	}
	r.out, r.mod = calcTables(defaultPoly, winSize)

	return r
}

// NewRabin returns default rabin Chunker.
func NewRabin() *rabin {
	return NewRabinWithParams(minSize, maxSize)
}

// Next implements Chunker interface.
func (r *rabin) Next(buf []byte) (*Chunk, error) {
	if r.end == 0 || r.bpos == r.end {
		if !r.updateBuf() {
			return nil, r.err
		}
	}

	r.start = r.bpos
	r.lastChunk = buf[:0]
	count := 1

	for ; count <= r.min; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		// if chunk is still less than minimal size
		// but more data needs to be read
		if r.bpos == r.end && count < r.min && !r.updateBuf() {
			return r.chomp()
		}
	}

	if r.digest&mask == 0 || r.bpos == r.end && !r.updateBuf() && r.err == io.EOF {
		return r.chunk()
	}

	for ; count <= r.max; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.digest&mask == 0 || count == r.max {
			return r.chunk()
		} else if r.bpos == r.end && !r.updateBuf() {
			return r.chomp()
		}
	}

	return r.chunk()
}

func (r *rabin) chomp() (*Chunk, error) {
	if r.err == nil || r.err == io.EOF {
		return r.chunk()
	}

	return nil, r.err
}

// updateBuf reads more data from buffer and returns
// true is some data was read.
func (r *rabin) updateBuf() bool {
	if r.err != nil {
		return false
	}

	r.lastChunk = append(r.lastChunk, r.buf[r.start:r.bpos]...)

	r.bpos = 0
	r.end = 0
	r.start = 0

	r.end, r.err = io.ReadFull(r.r, r.buf[:])

	if r.err == io.ErrUnexpectedEOF {
		r.err = io.EOF
	}

	return r.end != 0
}

func (r *rabin) chunk() (*Chunk, error) {
	r.lastChunk = append(r.lastChunk, r.buf[r.start:r.bpos]...)

	return &Chunk{
		Cut:  uint64(r.digest),
		Data: r.lastChunk,
	}, nil
}

func (r *rabin) append(b byte) {
	index := byte(r.digest >> r.shift)
	r.digest <<= 8
	r.digest |= Poly(b)
	r.digest ^= r.mod[index]
}

func (r *rabin) slide(b byte) {
	out := r.window[r.wpos]
	r.window[r.wpos] = b
	r.digest ^= r.out[out]
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
