package morsel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentIndependentRuns runs many differently-configured executors at
// once. Under -race it checks that no shared state leaks between runs.
func TestConcurrentIndependentRuns(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ex := NewExecutor(Config{
				MaxWorkers:    uint(1 + i%4),
				MorselSize:    uint(1 + i%7),
				QueueCapacity: uint(4 + i%8),
				StealAttempts: uint(1 + i%3),
			})
			const n = 5000
			var count atomic.Int64
			if err := ex.ForEachSlice(context.Background(), make([]int, n), func(int) error {
				count.Add(1)
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
			if count.Load() != n {
				t.Errorf("run %d: count = %d, want %d", i, count.Load(), n)
			}
		}(i)
	}
	wg.Wait()
}

// TestConcurrentRunsSharedExecutor runs many concurrent executions on the same
// Executor. Each run has its own engine state; only config and stats are
// shared, and stats are guarded.
func TestConcurrentRunsSharedExecutor(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 8, QueueCapacity: 16, StealAttempts: 2})
	const runs = 16
	const n = 2000

	var wg sync.WaitGroup
	for i := 0; i < runs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var count atomic.Int64
			if err := ex.ForEachSlice(context.Background(), make([]int, n), func(int) error {
				count.Add(1)
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
			if count.Load() != n {
				t.Errorf("count = %d, want %d", count.Load(), n)
			}
		}()
	}
	wg.Wait()

	wantMorsels := uint64(runs) * uint64((n+7)/8)
	if got := ex.Stats().MorselsExecuted; got != wantMorsels {
		t.Fatalf("morsels executed = %d, want %d", got, wantMorsels)
	}
}

// TestConcurrentCancelAndError mixes cancellation and callback errors across
// concurrent runs and checks each returns the right error.
func TestConcurrentCancelAndError(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			boom := errors.New("boom")
			var count atomic.Int64
			err := ex.ForEachSlice(ctx, make([]int, 500_000), func(int) error {
				if count.Add(1) == 500 {
					if i%2 == 0 {
						cancel()
					} else {
						return boom
					}
				}
				return nil
			})
			if i%2 == 0 {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("run %d: err = %v, want context.Canceled", i, err)
				}
				return
			}
			if !errors.Is(err, boom) {
				t.Errorf("run %d: err = %v, want boom", i, err)
			}
		}(i)
	}
	wg.Wait()
}

// TestFewMorselsUseMultipleWorkers guards the elasticity heuristic: a workload
// with fewer morsels than a queue can hold must still spread across workers,
// not run on a single one.
func TestFewMorselsUseMultipleWorkers(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 8, MorselSize: 256, QueueCapacity: 256, StealAttempts: 4})
	data := make([]int, 4*256) // exactly 4 morsels

	var current, peak atomic.Int32
	err := ex.ForEachSlice(context.Background(), data, func(int) error {
		c := current.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		for i := 0; i < 1_000_000; i++ {
			_ = i * i
		}
		current.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if peak.Load() < 2 {
		t.Fatalf("peak concurrency = %d, want >= 2 (few morsels ran on one worker)", peak.Load())
	}
}

// TestConcurrentPanicRecovery runs several panicking workloads concurrently
// with recovery enabled.
func TestConcurrentPanicRecovery(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ex := NewExecutor(Config{
				MaxWorkers: 4, MorselSize: 1, QueueCapacity: 4, StealAttempts: 2, RecoverPanics: true,
			})
			err := ex.ForEachSlice(context.Background(), make([]int, 10_000), func(int) error {
				panic("boom")
			})
			if err == nil {
				t.Error("want a recovered panic error")
			}
		}()
	}
	wg.Wait()
}
