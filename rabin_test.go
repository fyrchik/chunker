package chunker

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chunk struct {
	len    int
	digest uint64
}

var chunks = []chunk{
	{len: 1052085, digest: 0x000f9665a0f00000},
	{len: 851323, digest: 0x001fe07b10f00000},
	{len: 889109, digest: 0x00071a3b62400000},
	{len: 598360, digest: 0x001e4c88b6100000},
	{len: 605299, digest: 0x001ee84586000000},
	{len: 820265, digest: 0x00141426e4e00000},
	{len: 1075420, digest: 0x0018bb931fd00000},
	{len: 3291577, digest: 0x000bb3fc99900000},
	{len: 1663881, digest: 0x0004f0d2a3500000},
	{len: 2156442, digest: 0x00153e25df800000},
	{len: 972326, digest: 0x001dbb0692c00000},
	{len: 837692, digest: 0x001e4261eb900000},
	{len: 1963437, digest: 0x00071a431904a02e},
}

func TestRabin_Next(t *testing.T) {
	buf := getRandom(42, 16*MiB)
	r := NewRabin()
	gr := newGentleReaderFromBuf(buf)
	r.Reset(gr)

	var total int

	for i, ch := range chunks {
		c, err := r.Next(make([]byte, 2*MiB))
		require.NoError(t, err)
		require.NotNil(t, c, "chunk #%d is nil", i)
		assert.Equal(t, ch.len, len(c.Data), "chunk #%d length", i)
		assert.Equal(t, ch.digest, c.Digest, "chunk #%d digest", i)
		total += ch.len
	}

	require.Equal(t, len(buf), total)

	c, err := r.Next(make([]byte, 2*MiB))
	require.Equal(t, io.EOF, err)
	require.Nil(t, c)
	require.False(t, gr.Used)

	c, err = r.Next(nil)
	require.Equal(t, io.EOF, err)
	require.Nil(t, c)
	require.False(t, gr.Used)
}

func TestRabin_EmptyReader(t *testing.T) {
	r := NewRabin()
	r.Reset(bytes.NewReader(nil))

	c, err := r.Next(nil)
	require.Equal(t, io.EOF, err)
	require.Nil(t, c)
}

func TestRabin_SmallChunks(t *testing.T) {
	const (
		chunkSize = 16
		dataSize  = 128
	)

	buf := getRandom(42, dataSize)
	r := NewRabinWithParams(chunkSize/2, chunkSize)
	gr := newGentleReaderFromBuf(buf)
	r.Reset(gr)

	n := dataSize / chunkSize
	for i := 0; i < n; i++ {
		c, err := r.Next(make([]byte, KiB))
		require.NoError(t, err)
		require.NotNil(t, c, "chunk #%d is nil", i)
		require.Equal(t, chunkSize, len(c.Data), "chunk #%d length", i)
	}

	c, err := r.Next(make([]byte, 1))
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, c)
}

func TestRabin_BadReader(t *testing.T) {
	chunkSize := 2 * MiB
	buf := getRandom(2, 16*MiB)
	r := NewRabinWithParams(chunkSize, chunkSize)
	gr := &gentleReader{Reader: newErrorReaderFromBuf(3*MiB, buf)}
	r.Reset(gr)

	c, err := r.Next(nil)
	require.NoError(t, err)
	require.Equal(t, chunkSize, len(c.Data))

	c, err = r.Next(nil)
	require.Error(t, err)
	require.Nil(t, c)
	require.False(t, gr.Used)

	chunkSize = 4 * MiB
	r = NewRabinWithParams(chunkSize, chunkSize)
	gr = &gentleReader{Reader: newErrorReaderFromBuf(3*MiB, buf)}
	r.Reset(gr)

	c, err = r.Next(nil)
	require.Error(t, err)
	require.Nil(t, c)

	c, err = r.Next(nil)
	require.Error(t, err)
	require.Nil(t, c)
	require.False(t, gr.Used)
}

func TestRabin_MinSize(t *testing.T) {
	buf := getRandom(1, 100)
	r := NewRabin()
	gr := newGentleReaderFromBuf(buf)
	r.Reset(gr)

	c, err := r.Next(make([]byte, KiB))
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, 100, len(c.Data))
	require.EqualValues(t, 0x78a069e0967f2, c.Digest)

	c, err = r.Next(make([]byte, KiB))
	require.Equal(t, io.EOF, err)
	require.Nil(t, c)
	require.False(t, gr.Used)
}

func BenchmarkRabin_Next(b *testing.B) {
	b.ReportAllocs()

	b.Run("small chunks", func(b *testing.B) {
		const (
			chunkSize = 128
			dataSize  = 2 * KiB
		)

		buf := getRandom(1, dataSize)
		r := NewRabinWithParams(chunkSize/2, chunkSize)

		b.Run("no allocs", func(b *testing.B) {
			benchNoAllocs(b, r, buf)
		})

		b.Run("pre alloc", func(b *testing.B) {
			benchPreAlloc(b, r, buf, chunkSize)
		})
	})
}

func benchNoAllocs(b *testing.B, r *rabin, buf []byte) {
	var err error

	for i := 0; i < b.N; i++ {
		r.Reset(bytes.NewReader(buf))
		for err == nil {
			_, err = r.Next(nil)
		}
	}
}

func benchPreAlloc(b *testing.B, r *rabin, buf []byte, size int) {
	var err error

	for i := 0; i < b.N; i++ {
		r.Reset(bytes.NewReader(buf))
		for err == nil {
			_, err = r.Next(make([]byte, size))
		}
	}
}

func getRandom(seed int64, count int) []byte {
	buf := make([]byte, count)
	rnd := rand.New(rand.NewSource(seed))

	for i := 0; i < count; i += 4 {
		binary.LittleEndian.PutUint32(buf[i:], rnd.Uint32())
	}

	return buf
}
