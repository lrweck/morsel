package morsel

import (
	"fmt"
	"testing"
)

// TestRangeAllocationsTrackSlice asserts the producer no longer materializes an
// int slice per morsel: Range should allocate about as much as Slice, which
// never materializes elements. Before the descriptor change Range allocated one
// []int per morsel on top of this.
func TestRangeAllocationsTrackSlice(t *testing.T) {
	const n = 1 << 16
	data := make([]int, n)
	opts := []Option{MaxWorkers(4), MorselSize(64), QueueCapacity(16)}

	runRange := func() {
		if err := Range(0, n).ForEach(func(int) {}, opts...); err != nil {
			t.Fatal(err)
		}
	}
	runSlice := func() {
		if err := Slice(data).ForEach(func(int) {}, opts...); err != nil {
			t.Fatal(err)
		}
	}
	runRange()
	runSlice()

	aRange := testing.AllocsPerRun(5, runRange)
	aSlice := testing.AllocsPerRun(5, runSlice)
	if aRange > aSlice*1.25 {
		t.Fatalf("Range allocs = %.0f, Slice allocs = %.0f: per-morsel allocation is back", aRange, aSlice)
	}
}

// BenchmarkRangeAlloc compares Range with Slice, which never materializes
// elements. Before the descriptor change, Range was ~2.5x slower and allocated
// one int slice per morsel; after, it should track Slice.
func BenchmarkRangeAlloc(b *testing.B) {
	for _, n := range []int{1 << 14, 1 << 18, 1 << 22} {
		data := make([]int, n)
		for i := range data {
			data[i] = i
		}
		b.Run(fmt.Sprintf("n=%d/range", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := Range(0, n).ForEach(func(int) {}); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("n=%d/slice", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := Slice(data).ForEach(func(int) {}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRangeMap covers the transform path, where the materialized range is
// copied into a scratch output buffer.
func BenchmarkRangeMap(b *testing.B) {
	const n = 1 << 20
	b.ReportAllocs()
	for b.Loop() {
		if err := Range(0, n).Map(func(v int) int { return v * 2 }).ForEach(func(int) {}); err != nil {
			b.Fatal(err)
		}
	}
}
