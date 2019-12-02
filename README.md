# Content-Defined Chunking in Golang

This package provides various CDC methods in Go.

Currently implemented:
- Rabin chunking method
    - https://en.wikipedia.org/wiki/Rabin_fingerprint
    - https://github.com/fd0/rabin-cdc/
- Gear-based chunking
    - https://www.sciencedirect.com/science/article/pii/S0166531614000790

## Design
1. All work is done through `Chunker` interface. `Next` method should return `nil` error
iff a new chunk was consumed. Errors from underlying reader are returns unchanged
except `io.ErrUnexpectedEOF` which is replaced with `io.EOF`.
2. All chunkers are well-behaved in a sense that they don't use underlying `Reader` after
encountering any error.
    
## Usage
As simple as it seems:
```golang
r := bytes.NewReader(buf)
ch := NewRabin()
ch.Reset(r)
for chunk, err := r.Next(nil); err != nil {
    // process chunk
}
```

`Next` method can be provided with pre-allocated buffer for storing chunk data.

## TODO
- implement all methods from https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf
- implement `HashedChunk` parametrised by `hash.Hash`
for calculating cryptographic checksum of returned chunks