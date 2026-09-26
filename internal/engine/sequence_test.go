package engine

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

// TestRunSliceAssignsSequence checks that the producer assigns a contiguous
// logical sequence starting at zero, on both the pooled and sequential paths.
func TestRunSliceAssignsSequence(t *testing.T) {
	const (
		n    = 20
		size = 4
	)
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	for _, workers := range []uint{1, 4} {
		t.Run("workers"+strconv.Itoa(int(workers)), func(t *testing.T) {
			cfg := Config{MaxWorkers: workers, MorselSize: size, QueueCapacity: 4, StealAttempts: 2}
			var mu sync.Mutex
			startBySeq := map[uint64]int{}
			process := func(_ *Worker[int, struct{}], m Work[int]) error {
				mu.Lock()
				startBySeq[m.Sequence] = m.Start
				mu.Unlock()
				return nil
			}
			_, _, err := RunSlice(cfg, context.Background(), data, process,
				func() struct{} { return struct{}{} }, func(*struct{}, struct{}) {})
			if err != nil {
				t.Fatal(err)
			}
			if want := n / size; len(startBySeq) != want {
				t.Fatalf("morsels = %d, want %d", len(startBySeq), want)
			}
			for seq := range uint64(n / size) {
				if got, want := startBySeq[seq], int(seq)*size; got != want {
					t.Fatalf("sequence %d: Start = %d, want %d", seq, got, want)
				}
			}
		})
	}
}

// TestSequenceDecouplesOrderFromStart covers the case MapSliceBatch relies on:
// a source whose Start is not a logical offset. The sequence still indexes the
// morsel's source order, which is what parts[Sequence] uses.
func TestSequenceDecouplesOrderFromStart(t *testing.T) {
	// Deliberately non-logical Start values, in no particular order.
	startBySeq := map[uint64]int{0: 500, 1: -3, 2: 77, 3: 12}
	cfg := Config{MaxWorkers: 4, MorselSize: 8, QueueCapacity: 16, StealAttempts: 2}.Normalize()
	parts := make([][]int, len(startBySeq))
	var mu sync.Mutex
	r := NewRunner[int, struct{}](cfg, t.Context(),
		func(_ *Worker[int, struct{}], m Work[int]) error {
			mu.Lock()
			parts[m.Sequence] = m.Items
			mu.Unlock()
			return nil
		},
		func() struct{} { return struct{}{} },
	)
	for seq := uint64(0); seq < uint64(len(startBySeq)); seq++ {
		if !r.Publish(Work[int]{Start: startBySeq[seq], Sequence: seq, Items: []int{int(seq)}}) {
			t.Fatal("publish failed")
		}
	}
	r.Done()
	if _, err := r.Wait(); err != nil {
		t.Fatal(err)
	}
	for seq, start := range startBySeq {
		if got := parts[seq]; len(got) != 1 || got[0] != int(seq) {
			t.Fatalf("parts[%d] = %v (Start was %d), want [%d]", seq, got, start, seq)
		}
	}
}
