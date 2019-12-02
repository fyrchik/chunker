package chunker

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLazyBuf_Next(t *testing.T) {
	buf := getRandom(0, 32*MiB)

	var result []byte
	lb := &LazyBuf{
		Reader: bytes.NewReader(buf),
		Pos:    0,
		end:    0,
		onUpdate: func(b *LazyBuf) {
			result = append(result, b.Buf[:]...)
		},
	}

	lb.Update()

	for lb.err == nil {
		_ = lb.Next()
	}

	require.Equal(t, len(buf), len(result))
	require.Equal(t, buf, result)
}
