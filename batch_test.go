package morsel

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestBatchForEachSlice(t *testing.T) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	ex := testExecutor()
	var batches atomic.Int64
	var sum atomic.Int64
	err := ex.ForEachSliceBatch(context.Background(), data, func(b []int) error {
		batches.Add(1)
		s := 0
		for _, v := range b {
			s += v
		}
		sum.Add(int64(s))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(999 * 1000 / 2); sum.Load() != want {
		t.Fatalf("sum = %d, want %d", sum.Load(), want)
	}
	if batches.Load() == 0 || batches.Load() >= 1000 {
		t.Fatalf("batches = %d, want batching (1 < n < 1000)", batches.Load())
	}
}

func TestBatchErgonomicAndSeq(t *testing.T) {
	var count atomic.Int64
	err := ForEachBatch([]int{1, 2, 3, 4}, func(b []int) { count.Add(int64(len(b))) }, MorselSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 4 {
		t.Fatalf("count = %d, want 4", count.Load())
	}

	ex := testExecutor()
	var seqCount atomic.Int64
	err = ex.ForEachSeqBatch(context.Background(), func(yield func(int) bool) {
		for i := range 100 {
			if !yield(i) {
				return
			}
		}
	}, func(b []int) error { seqCount.Add(int64(len(b))); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if seqCount.Load() != 100 {
		t.Fatalf("seq count = %d, want 100", seqCount.Load())
	}

	boom := errors.New("iter")
	err = ex.ForEachSeqErrBatch(context.Background(), func(yield func(int, error) bool) {
		yield(1, nil)
		yield(0, boom)
	}, func([]int) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestBatchErrorAborts(t *testing.T) {
	boom := errors.New("boom")
	ex := testExecutor()
	err := ex.ForEachSliceBatch(context.Background(), make([]int, 1000), func([]int) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if err := ForEachEBatch([]int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatalf("ForEachEBatch nil: err = %v", err)
	}
	if err := ForEachBatch([]int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatalf("ForEachBatch nil: err = %v", err)
	}
}

func TestBatchMapSliceOrderAndFilter(t *testing.T) {
	data := make([]int, 100)
	for i := range data {
		data[i] = i
	}
	ex := testExecutor()
	got, err := ex.MapSliceBatch(context.Background(), data, func(b []int) []int {
		out := make([]int, len(b))
		for i, v := range b {
			out[i] = v * 2
		}
		return out
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range got {
		if v != i*2 {
			t.Fatalf("got[%d] = %d, want %d", i, v, i*2)
		}
	}

	// Variable length per batch stays in order (filter-like).
	got, err = ex.MapSliceBatch(context.Background(), data, func(b []int) []int {
		var out []int
		for _, v := range b {
			if v%2 == 0 {
				out = append(out, v)
			}
		}
		return out
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || got[0] != 0 || got[49] != 98 {
		t.Fatalf("filtered = len %d bounds %v", len(got), got[:min(3, len(got))])
	}
}

func TestMapSliceBatchExactOrder(t *testing.T) {
	for _, tc := range []struct{ n, morsel int }{
		{0, 4}, {1, 4}, {3, 4}, {4, 4}, {5, 4}, {9, 4}, {100, 8}, {1000, 256},
	} {
		data := make([]int, tc.n)
		for i := range data {
			data[i] = i
		}
		ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: uint(tc.morsel), QueueCapacity: 64, StealAttempts: 4})
		got, err := ex.MapSliceBatch(context.Background(), data, func(b []int) []int {
			// Two outputs per input element and a partial last morsel, so a
			// wrong slot shows up as misordered elements.
			out := make([]int, 0, 2*len(b))
			for _, v := range b {
				out = append(out, v, v)
			}
			return out
		})
		if err != nil {
			t.Fatalf("n=%d morsel=%d: %v", tc.n, tc.morsel, err)
		}
		if want := 2 * tc.n; len(got) != want {
			t.Fatalf("n=%d morsel=%d: len = %d, want %d", tc.n, tc.morsel, len(got), want)
		}
		for i, v := range got {
			if want := i / 2; v != want {
				t.Fatalf("n=%d morsel=%d: got[%d] = %d, want %d", tc.n, tc.morsel, i, v, want)
			}
		}
	}
}

func TestBatchMapSeqAndReduce(t *testing.T) {
	ex := testExecutor()
	seq := func(yield func(int) bool) {
		for i := 1; i <= 100; i++ {
			if !yield(i) {
				return
			}
		}
	}
	got, err := ex.MapSeqBatch(context.Background(), seq, func(b []int) []int {
		out := make([]int, len(b))
		for i, v := range b {
			out[i] = v * 2
		}
		return out
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("len = %d, want 100", len(got))
	}
	slices.Sort(got)
	for i, v := range got {
		if v != (i+1)*2 {
			t.Fatalf("got[%d] = %d, want %d", i, v, (i+1)*2)
		}
	}

	data := make([]int, 1000)
	for i := range data {
		data[i] = i + 1
	}
	add := func(a, b int) int { return a + b }
	sumBatch := func(acc int, b []int) int {
		for _, v := range b {
			acc += v
		}
		return acc
	}
	sum, err := ex.ReduceSliceBatch(context.Background(), data, 0, sumBatch, add)
	if err != nil {
		t.Fatal(err)
	}
	if want := 1000 * 1001 / 2; sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
	sumSeq, err := ex.ReduceSeqBatch(context.Background(), seq, 0, sumBatch, add)
	if err != nil {
		t.Fatal(err)
	}
	if want := 100 * 101 / 2; sumSeq != want {
		t.Fatalf("seq sum = %d, want %d", sumSeq, want)
	}
}

func TestBatchPipeline(t *testing.T) {
	got, err := Slice([]int{0, 1, 2, 3, 4, 5, 6, 7}).
		MapBatch(func(b []int) []int {
			out := make([]int, len(b))
			for i, v := range b {
				out[i] = v * 2
			}
			return out
		}).
		FilterBatch(func(b []int) []int {
			var out []int
			for _, v := range b {
				if v%4 == 0 {
					out = append(out, v)
				}
			}
			return out
		}).
		Collect(MorselSize(2))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	if !slices.Equal(got, []int{0, 4, 8, 12}) {
		t.Fatalf("got = %v", got)
	}

	doubled, err := Range(0, 8).
		FlatMapBatch(func(b []int) []int {
			out := make([]int, 0, 2*len(b))
			for _, v := range b {
				out = append(out, v, v)
			}
			return out
		}).
		Collect(MorselSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(doubled) != 16 {
		t.Fatalf("len = %d, want 16", len(doubled))
	}

	var batches atomic.Int64
	var seen atomic.Int64
	err = Range(0, 100).
		ForEachBatch(func(b []int) {
			batches.Add(1)
			seen.Add(int64(len(b)))
		}, MorselSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 100 {
		t.Fatalf("seen = %d, want 100", seen.Load())
	}
	if batches.Load() == 0 || batches.Load() >= 100 {
		t.Fatalf("batches = %d, want batching", batches.Load())
	}

	total, err := Slice([]int{1, 2, 3, 4}).
		MapBatch(func(b []int) []int { return b }).
		ReduceBatch(0,
			func(acc int, b []int) int {
				for _, v := range b {
					acc += v
				}
				return acc
			},
			func(a, b int) int { return a + b })
	if err != nil {
		t.Fatal(err)
	}
	if total != 10 {
		t.Fatalf("total = %d, want 10", total)
	}
}

func TestEagerSequentialPerItem(t *testing.T) {
	// MaxWorkers 1 runs on the caller with no queues, so an eager run
	// publishes every item immediately. Deterministic: single-threaded.
	ex := NewExecutor(Config{MaxWorkers: 1, MorselSize: 8, Eager: true})
	var count atomic.Int64
	err := ex.ForEachSeq(context.Background(), func(yield func(int) bool) {
		for i := range 10 {
			if !yield(i) {
				return
			}
		}
	}, func(int) error { count.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 10 {
		t.Fatalf("count = %d, want 10", count.Load())
	}
	if got := ex.Stats().MorselsExecuted; got != 10 {
		t.Fatalf("morsels = %d, want 10", got)
	}
}

func TestEagerRendezvous(t *testing.T) {
	// The producer waits for each item to be processed before yielding the
	// next, so every arrival finds no backlog and publishes a 1-item
	// morsel — observed through the batch API. Without Eager this deadlocks
	// (the producer holds a partial morsel forever). The ctx timeout only
	// fires on regression: success is synchronization, not timing.
	run := func(t *testing.T, do func(ctx context.Context, seq func(func(int) bool), fn func([]int) error) error) {
		t.Helper()
		ack := make(chan struct{})
		var count atomic.Int64
		seq := func(yield func(int) bool) {
			for i := range 10 {
				if !yield(i) {
					return
				}
				<-ack
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := do(ctx, seq, func(b []int) error {
			count.Add(1)
			ack <- struct{}{}
			if len(b) != 1 {
				return errors.New("want single-item morsel")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if count.Load() != 10 {
			t.Fatalf("count = %d, want 10", count.Load())
		}
	}

	t.Run("executor", func(t *testing.T) {
		ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 8, QueueCapacity: 64, StealAttempts: 4, Eager: true})
		run(t, func(ctx context.Context, seq func(func(int) bool), fn func([]int) error) error {
			return ex.ForEachSeqBatch(ctx, seq, fn)
		})
	})
	t.Run("pipeline", func(t *testing.T) {
		run(t, func(ctx context.Context, seq func(func(int) bool), fn func([]int) error) error {
			return Iter(seq).ForEachEBatch(fn, WithContext(ctx), MorselSize(8), Eager(true))
		})
	})
}

func TestEagerBulkExactOnce(t *testing.T) {
	// Framing under load is timing-dependent, but every item must still be
	// processed exactly once.
	const total = 50_000
	ex := NewExecutor(Config{MaxWorkers: 8, MorselSize: 256, QueueCapacity: 64, StealAttempts: 8, Eager: true})
	seen := make([]atomic.Int32, total)
	err := ex.ForEachSeq(context.Background(), func(yield func(int) bool) {
		for i := range total {
			if !yield(i) {
				return
			}
		}
	}, func(v int) error { seen[v].Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	for i := range seen {
		if got := seen[i].Load(); got != 1 {
			t.Fatalf("element %d seen %d times, want 1", i, got)
		}
	}
}
