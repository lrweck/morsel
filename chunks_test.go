package morsel

import (
	"bytes"
	"sync/atomic"
	"testing"
)

// TestChunksPooledMatchesChunks checks that recycling the chunk buffers does
// not change the bytes the callback sees. It runs through the pool (possibly
// many workers), so -race also covers buffer reuse while other morsels are in
// flight.
func TestChunksPooledMatchesChunks(t *testing.T) {
	data := make([]byte, 100_000)
	for i := range data {
		data[i] = byte(i*7 + i/13)
	}

	sum := func(p Pipeline[[]byte, []byte]) int64 {
		t.Helper()
		var s atomic.Int64
		if err := p.ForEach(func(b []byte) {
			var local int64
			for _, c := range b {
				local += int64(c)
			}
			s.Add(local)
		}); err != nil {
			t.Fatal(err)
		}
		return s.Load()
	}

	owned := sum(Chunks(bytes.NewReader(data), 4096))
	pooled := sum(ChunksPooled(bytes.NewReader(data), 4096))
	if owned != pooled {
		t.Fatalf("ChunksPooled sum = %d, Chunks sum = %d", pooled, owned)
	}
}
