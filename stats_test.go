package morsel

import (
	"context"
	"sync"
	"testing"

	"github.com/lrweck/morsel/internal/engine"
)

// TestStatsConcurrentReadAndExecutions runs many executions on one Executor
// while a reader repeatedly snapshots Stats. Counters are independent atomics,
// so every field must be monotonically non-decreasing, and once every run has
// finished the totals must be exact.
func TestStatsConcurrentReadAndExecutions(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 4, MorselSize: 8, QueueCapacity: 16, StealAttempts: 2})
	const (
		runs = 8
		n    = 2000
	)

	done := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		last := ex.Stats()
		for {
			select {
			case <-done:
				return
			default:
			}
			cur := ex.Stats()
			switch {
			case cur.MorselsCreated < last.MorselsCreated,
				cur.MorselsExecuted < last.MorselsExecuted,
				cur.StealsAttempted < last.StealsAttempted,
				cur.StealsSucceeded < last.StealsSucceeded,
				cur.WorkersCreated < last.WorkersCreated:
				t.Errorf("stats decreased: %+v -> %+v", last, cur)
				return
			}
			last = cur
		}
	}()

	var wg sync.WaitGroup
	for range runs {
		wg.Go(func() {
			if err := ex.ForEachSlice(context.Background(), make([]int, n), func(int) error {
				return nil
			}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	close(done)
	reader.Wait()

	want := uint64(runs) * uint64((n+7)/8)
	if got := ex.Stats(); got.MorselsExecuted != want || got.MorselsCreated != want {
		t.Fatalf("stats = %+v, want %d created and executed morsels", got, want)
	}
}

// mutexStats is the pre-atomic aggregate representation, kept only so the
// benchmark can compare it against the atomic one.
type mutexStats struct {
	mu sync.Mutex
	s  engine.Stats
}

func (m *mutexStats) record(s engine.Stats) {
	m.mu.Lock()
	m.s.MorselsCreated += s.MorselsCreated
	m.s.MorselsExecuted += s.MorselsExecuted
	m.s.StealsAttempted += s.StealsAttempted
	m.s.StealsSucceeded += s.StealsSucceeded
	m.s.WorkersCreated += s.WorkersCreated
	m.mu.Unlock()
}

func (m *mutexStats) snapshot() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Stats{
		MorselsCreated:  m.s.MorselsCreated,
		MorselsExecuted: m.s.MorselsExecuted,
		StealsAttempted: m.s.StealsAttempted,
		StealsSucceeded: m.s.StealsSucceeded,
		WorkersCreated:  m.s.WorkersCreated,
	}
}

// BenchmarkExecutorStats compares the old mutex aggregate with the atomic one
// on the per-run record + snapshot path. "short" models a single tiny run;
// "parallel" models many short runs finishing concurrently, where the mutex
// serializes and the atomics do not.
func BenchmarkExecutorStats(b *testing.B) {
	s := engine.Stats{MorselsCreated: 1, MorselsExecuted: 1, StealsAttempted: 1, StealsSucceeded: 1, WorkersCreated: 1}

	b.Run("short/atomic", func(b *testing.B) {
		ex := NewExecutor(Config{})
		b.ReportAllocs()
		for b.Loop() {
			ex.record(s)
			_ = ex.Stats()
		}
	})
	b.Run("short/mutex", func(b *testing.B) {
		var m mutexStats
		b.ReportAllocs()
		for b.Loop() {
			m.record(s)
			_ = m.snapshot()
		}
	})
	b.Run("parallel/atomic", func(b *testing.B) {
		ex := NewExecutor(Config{})
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ex.record(s)
				_ = ex.Stats()
			}
		})
	})
	b.Run("parallel/mutex", func(b *testing.B) {
		var m mutexStats
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				m.record(s)
				_ = m.snapshot()
			}
		})
	})
}
