package morsel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// TestSmallInputRunsSequentially checks the fast path: an input that fits in a
// single morsel never spawns a worker or a queue.
func TestSmallInputRunsSequentially(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 8, MorselSize: 256})
	var count atomic.Int64
	if err := ex.ForEachSlice(context.Background(), make([]int, 100), func(int) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 100 {
		t.Fatalf("count = %d, want 100", count.Load())
	}
	if s := ex.Stats(); s.WorkersCreated != 0 {
		t.Fatalf("workers created = %d, want 0 (a single morsel needs no pool)", s.WorkersCreated)
	}
}

// TestMaxWorkersOneRunsSequentially: an explicit single worker skips the pool.
func TestMaxWorkersOneRunsSequentially(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 1, MorselSize: 1})
	var count atomic.Int64
	if err := ex.ForEachSlice(context.Background(), make([]int, 1000), func(int) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1000 {
		t.Fatalf("count = %d, want 1000", count.Load())
	}
	if s := ex.Stats(); s.WorkersCreated != 0 {
		t.Fatalf("workers created = %d, want 0", s.WorkersCreated)
	}
}

func TestSequentialRecoversPanic(t *testing.T) {
	ex := NewExecutor(Config{RecoverPanics: true})
	err := ex.ForEachSlice(context.Background(), []int{1, 2}, func(int) error { panic("boom") })
	if err == nil {
		t.Fatal("want a recovered panic error")
	}
}

func TestSequentialErrorAndCancel(t *testing.T) {
	boom := errors.New("boom")
	ex := NewExecutor(Config{MaxWorkers: 1, MorselSize: 1})

	count := 0
	err := ex.ForEachSlice(context.Background(), make([]int, 100), func(int) error {
		count++
		if count == 3 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3 (should stop at the first error)", count)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ex.ForEachSlice(ctx, make([]int, 100), func(int) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestPipelineSmallInputRunsSequentially(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 8, MorselSize: 256})
	total, err := Slice([]int{1, 2, 3, 4}).
		Map(func(v int) int { return v * 2 }).
		Reduce(0, func(a, v int) int { return a + v }, func(a, b int) int { return a + b },
			WithExecutor(ex))
	if err != nil {
		t.Fatal(err)
	}
	if total != 20 {
		t.Fatalf("total = %d, want 20", total)
	}
	if s := ex.Stats(); s.WorkersCreated != 0 {
		t.Fatalf("workers created = %d, want 0", s.WorkersCreated)
	}
}

func TestPipelineMaxWorkersOne(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 1, MorselSize: 4})
	got, err := Range(0, 100).Collect(WithExecutor(ex))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("len = %d, want 100", len(got))
	}
	if s := ex.Stats(); s.WorkersCreated != 0 {
		t.Fatalf("workers created = %d, want 0", s.WorkersCreated)
	}
}

// TestLargeInputStillUsesPool guards the other side: once the input exceeds one
// morsel, the pool is used.
func TestLargeInputStillUsesPool(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 64})
	var count atomic.Int64
	if err := ex.ForEachSlice(context.Background(), make([]int, 64*8), func(int) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 64*8 {
		t.Fatalf("count = %d, want %d", count.Load(), 64*8)
	}
	if s := ex.Stats(); s.WorkersCreated == 0 {
		t.Fatal("workers created = 0, want the pool for a multi-morsel input")
	}
}
