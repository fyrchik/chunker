package chunker

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type impl struct {
	name string
	new  func() Chunker
}

var implToBench = []impl{
	{"rabin", func() Chunker { return NewRabin() }},
}

var implToTest = append(implToBench, []impl{
	{"rabin small", func() Chunker {
		return NewRabinWithParams(16, 32)
	}},
	{"rabin bad boundaries", func() Chunker {
		return NewRabinWithParams(11, 71)
	}},
}...)

func TestChunker_Correct(t *testing.T) {
	buf := getRandom(1, 128)

	for _, impl := range implToTest {
		t.Run(impl.name, func(t *testing.T) {
			gr := newGentleReaderFromBuf(buf)
			ch := impl.new()
			ch.Reset(gr)

			var chunks []*Chunk

			for {
				c, err := ch.Next(nil)
				if err != nil {
					break
				}

				require.NotNil(t, c)

				chunks = append(chunks, c)
			}

			var result []byte
			for i := range chunks {
				result = append(result, chunks[i].Data...)
			}

			require.Equal(t, buf, result)
		})
	}
}

func BenchmarkChunker(b *testing.B) {
	const allocSize = 1 * MiB

	buf := getRandom(1, 32*MiB)

	b.ReportAllocs()

	for _, impl := range implToBench {
		b.Run(impl.name+"/nil", func(b *testing.B) {
			benchNoAllocs(b, impl.new(), buf)
		})

		b.Run(impl.name+"/prealloc", func(b *testing.B) {
			benchPreAlloc(b, impl.new(), buf, allocSize)
		})
	}
}

func benchNoAllocs(b *testing.B, ch Chunker, buf []byte) {
	var err error

	for i := 0; i < b.N; i++ {
		ch.Reset(bytes.NewReader(buf))
		for err == nil {
			_, err = ch.Next(nil)
		}
	}
}

func benchPreAlloc(b *testing.B, ch Chunker, buf []byte, size int) {
	var err error

	for i := 0; i < b.N; i++ {
		ch.Reset(bytes.NewReader(buf))
		for err == nil {
			_, err = ch.Next(make([]byte, size))
		}
	}
}
