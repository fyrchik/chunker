package chunker

import (
	"io"
	"math/bits"
)

const (
	defaultPoly = Poly(0x3DA3358B4DC173)
	minSize     = 1 << 19
	maxSize     = 1 << 23
	avgSize     = 1 << 20
	mask        = avgSize - 1
	winSize     = 64
	bufSize     = 2 * MiB
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
	r      io.Reader

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
	r.bpos = bufSize
	r.end = bufSize
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
	var err error

	buf = buf[:0]

	if r.bpos == r.end {
		if err = r.updateBuf(); err != nil && err != io.EOF {
			return nil, err
		}
	}

	count := 1
	for ; count < minSize; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.bpos == r.end {
			if err == io.EOF {
				break
			}

			buf = append(buf, r.buf[r.start:]...)

			if err = r.updateBuf(); err != nil && err != io.EOF {
				return nil, err
			}
		}
	}

	if r.digest&mask == 0 || (r.bpos == r.end && err == io.EOF) {
		if r.digest&mask == 0 {
			err = nil
		}

		return &Chunk{
			Length: count,
			Digest: uint64(r.digest),
			Data:   buf,
		}, err
	}

	for ; count < maxSize; count++ {
		r.slide(r.buf[r.bpos])
		r.bpos++

		if r.digest&mask == 0 {
			return &Chunk{
				Length: count,
				Digest: uint64(r.digest),
				Data:   append(buf, r.buf[r.start:r.bpos]...),
			}, nil
		} else if r.bpos == r.end {
			if err = r.updateBuf(); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
		}
	}

	return &Chunk{
		Length: count,
		Digest: uint64(r.digest),
		Data:   append(buf, buf[r.start:r.end]...),
	}, nil
}

func (r *rabin) updateBuf() (err error) {
	r.bpos = 0
	r.start = 0
	r.end, err = io.ReadFull(r.r, r.buf[:])

	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}

	return err
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
