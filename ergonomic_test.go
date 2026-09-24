package morsel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestForEachSeqErgonomic(t *testing.T) {
	var count atomic.Int64
	err := ForEachSeq(func(yield func(int) bool) {
		for i := 0; i < 100; i++ {
			if !yield(i) {
				return
			}
		}
	}, func(int) { count.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 100 {
		t.Fatalf("count = %d, want 100", count.Load())
	}
}

func TestForEachSeqEErgonomic(t *testing.T) {
	boom := errors.New("boom")
	err := ForEachSeqE(func(yield func(int) bool) {
		for i := 0; i < 100; i++ {
			if !yield(i) {
				return
			}
		}
	}, func(v int) error {
		if v == 3 {
			return boom
		}
		return nil
	}, MorselSize(1))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestForEachSeqErrErgonomic(t *testing.T) {
	boom := errors.New("iter")
	err := ForEachSeqErr(func(yield func(int, error) bool) {
		if !yield(1, nil) {
			return
		}
		yield(0, boom)
	}, func(int) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestOptionsCovered(t *testing.T) {
	var sum atomic.Int64
	err := ForEach([]int{1, 2, 3, 4}, func(v int) { sum.Add(int64(v)) },
		QueueCapacity(4), StealAttempts(2), MaxWorkers(2), MorselSize(1))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Load() != 10 {
		t.Fatalf("sum = %d, want 10", sum.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = ForEach([]int{1, 2, 3}, func(int) {}, WithContext(ctx))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	err = ForEach([]int{1}, func(int) { panic("boom") }, RecoverPanics(true))
	if err == nil {
		t.Fatal("want a recovered panic error")
	}
}

func TestForEachSeqErrCallbackError(t *testing.T) {
	boom := errors.New("callback")
	ex := testExecutor()
	err := ex.ForEachSeqErr(context.Background(), func(yield func(int, error) bool) {
		yield(1, nil)
	}, func(int) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestZeroValueConfig(t *testing.T) {
	ex := NewExecutor(Config{})
	cfg := ex.Config()
	if cfg.MaxWorkers <= 0 || cfg.MorselSize <= 0 || cfg.QueueCapacity <= 0 || cfg.StealAttempts <= 0 {
		t.Fatalf("config not normalized: %+v", cfg)
	}
	var count atomic.Int64
	if err := ex.ForEachSlice(context.Background(), []int{1, 2, 3}, func(int) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 3 {
		t.Fatalf("count = %d, want 3", count.Load())
	}
}

func TestPackageLevelPrimitives(t *testing.T) {
	ex := testExecutor()
	ctx := context.Background()
	data := []int{1, 2, 3, 4, 5}
	add := func(a, b int) int { return a + b }
	seq := func(yield func(int) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i) {
				return
			}
		}
	}

	if err := ForEachSlice(ctx, ex, data, func(int) error { return nil }); err != nil {
		t.Fatal(err)
	}

	out, err := MapSlice(ctx, ex, data, func(v int) int { return v * 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || out[4] != 10 {
		t.Fatalf("MapSlice = %v", out)
	}

	seqOut, err := MapSeq(ctx, ex, seq, func(v int) int { return v })
	if err != nil {
		t.Fatal(err)
	}
	if len(seqOut) != 5 {
		t.Fatalf("MapSeq len = %d, want 5", len(seqOut))
	}

	sum, err := ReduceSlice(ctx, ex, data, 0, add, add)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 15 {
		t.Fatalf("ReduceSlice = %d, want 15", sum)
	}

	sumSeq, err := ReduceSeq(ctx, ex, seq, 0, add, add)
	if err != nil {
		t.Fatal(err)
	}
	if sumSeq != 10 {
		t.Fatalf("ReduceSeq = %d, want 10", sumSeq)
	}
}

func TestNilFunctionVariants(t *testing.T) {
	ex := testExecutor()
	ctx := context.Background()
	seq := func(yield func(int) bool) {}
	seq2 := func(yield func(int, error) bool) {}

	if err := ForEach[int]([]int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEach")
	}
	if err := ForEachE[int]([]int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEachE")
	}
	if err := ForEachSeq[int](seq, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEachSeq")
	}
	if err := ForEachSeqE[int](seq, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEachSeqE")
	}
	if err := ForEachSeqErr[int](seq2, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEachSeqErr")
	}
	if err := ex.ForEachSlice(ctx, []int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("Executor.ForEachSlice")
	}
	if err := ex.ForEachSeq(ctx, seq, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("Executor.ForEachSeq")
	}
	if _, err := ex.MapSlice[int, int](ctx, []int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("MapSlice")
	}
	if _, err := ex.MapSeq[int, int](ctx, seq, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("MapSeq")
	}
	if _, err := ex.ReduceSlice[int, int](ctx, []int{1}, 0, nil, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ReduceSlice")
	}
	if _, err := ex.ReduceSeq[int, int](ctx, seq, 0, nil, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ReduceSeq")
	}
}

func TestCancelMidRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2})
	var count atomic.Int64
	err := ex.ForEachSlice(ctx, make([]int, 1_000_000), func(int) error {
		if count.Add(1) == 100 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if count.Load() >= 1_000_000 {
		t.Fatal("run was not cancelled")
	}
}

func TestErrorAbortMidRun(t *testing.T) {
	boom := errors.New("boom")
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2})
	var count atomic.Int64
	err := ex.ForEachSlice(context.Background(), make([]int, 1_000_000), func(int) error {
		if count.Add(1) == 50 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if count.Load() >= 1_000_000 {
		t.Fatal("run was not aborted")
	}
}

func TestPanicAsErrorRecovered(t *testing.T) {
	boom := errors.New("boom")
	ex := NewExecutor(Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2, RecoverPanics: true})
	err := ex.ForEachSlice(context.Background(), make([]int, 1000), func(int) error { panic(boom) })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestSmallQueuesBackpressure(t *testing.T) {
	// MaxWorkers=1 and QueueCapacity=1 force the producer through the overflow
	// queue and the space-blocking path.
	ex := NewExecutor(Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 1, StealAttempts: 1})
	var count atomic.Int64
	if err := ex.ForEachSlice(context.Background(), make([]int, 2000), func(int) error {
		count.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2000 {
		t.Fatalf("count = %d, want 2000", count.Load())
	}
}

func TestStreamCancelMidway(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2})
	var count atomic.Int64
	seq := func(yield func(int) bool) {
		for i := 0; i < 1_000_000; i++ {
			if !yield(i) {
				return
			}
		}
	}
	err := ex.ForEachSeq(ctx, seq, func(int) error {
		if count.Add(1) == 100 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if count.Load() >= 1_000_000 {
		t.Fatal("stream was not cancelled")
	}
}

func TestMapCancelPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ex := testExecutor()

	if _, err := ex.MapSlice(ctx, []int{1, 2, 3}, func(v int) int { return v }); !errors.Is(err, context.Canceled) {
		t.Fatalf("MapSlice: err = %v, want context.Canceled", err)
	}
	seq := func(yield func(int) bool) { yield(1) }
	if _, err := ex.MapSeq(ctx, seq, func(v int) int { return v }); !errors.Is(err, context.Canceled) {
		t.Fatalf("MapSeq: err = %v, want context.Canceled", err)
	}
	if _, err := ex.ReduceSeq(ctx, seq, 0, func(a, b int) int { return a + b }, func(a, b int) int { return a + b }); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReduceSeq: err = %v, want context.Canceled", err)
	}
}
