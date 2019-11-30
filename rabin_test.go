package chunker

import (
	"encoding/binary"
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

func TestRabin_nextChunk(t *testing.T) {
	buf := getRandom(42, 16*MiB)
	r := NewRabin()
	r.Reset(buf)

	var total int

	for i, ch := range chunks {
		c, err := r.Next(make([]byte, 2*MiB))
		assert.NoError(t, err)
		assert.Equal(t, ch.len, c.Length, "chunk #%d length", i)
		assert.Equal(t, ch.digest, c.Digest, "chunk #%d digest", i)
		total += ch.len
	}

	require.Equal(t, len(buf), total)
}

func getRandom(seed int64, count int) []byte {
	buf := make([]byte, count)
	rnd := rand.New(rand.NewSource(seed))

	for i := 0; i < count; i += 4 {
		binary.LittleEndian.PutUint32(buf[i:], rnd.Uint32())
	}

	return buf
}
