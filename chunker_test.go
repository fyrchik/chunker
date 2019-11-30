package chunker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var chunkerImpls = []struct {
	name string
	new  func() Chunker
}{
	{"rabin", func() Chunker { return NewRabin() }},
	{"rabin small", func() Chunker {
		return NewRabinWithParams(16, 32)
	}},
	{"rabin bad boundaries", func() Chunker {
		return NewRabinWithParams(11, 71)
	}},
}

func TestChunker_Correct(t *testing.T) {
	buf := getRandom(1, 128)

	for _, impl := range chunkerImpls {
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
