package morsel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestAdaptiveConfigPlumbing checks the adaptive knobs survive a round trip
// through the executor configuration.
func TestAdaptiveConfigPlumbing(t *testing.T) {
	ex := NewExecutor(Config{
		AdaptiveMorselSize: true,
		MinMorselSize:      32,
		MaxMorselSize:      4096,
		TargetMorselTime:   500 * time.Microsecond,
	})
	got := ex.Config()
	if !got.AdaptiveMorselSize || got.MinMorselSize != 32 || got.MaxMorselSize != 4096 || got.TargetMorselTime != 500*time.Microsecond {
		t.Fatalf("adaptive config round trip = %+v", got)
	}
}

// TestAdaptiveResultsCorrect runs a representative slice workload with adaptive
// sizing enabled and checks every element is processed exactly once, in order.
func TestAdaptiveResultsCorrect(t *testing.T) {
	const n = 100_000
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	ex := NewExecutor(Config{
		MaxWorkers:         8,
		MorselSize:         16,
		QueueCapacity:      16,
		StealAttempts:      4,
		AdaptiveMorselSize: true,
		MinMorselSize:      16,
		MaxMorselSize:      4096,
		TargetMorselTime:   100 * time.Microsecond,
	})
	got, err := ex.MapSlice(context.Background(), data, func(v int) int { return v * 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	for i, v := range got {
		if v != i*2 {
			t.Fatalf("result[%d] = %d, want %d", i, v, i*2)
		}
	}
}

// TestAdaptiveBatchStaysOrdered guards the MapSliceBatch index contract: parts
// are placed by m.Start/size, so adaptive sizing must not move boundaries.
func TestAdaptiveBatchStaysOrdered(t *testing.T) {
	const n = 4096
	data := make([]int, n)
	for i := range data {
		data[i] = i
	}
	ex := NewExecutor(Config{
		MaxWorkers:         8,
		MorselSize:         64,
		QueueCapacity:      16,
		StealAttempts:      4,
		AdaptiveMorselSize: true,
		MinMorselSize:      16,
		MaxMorselSize:      4096,
		TargetMorselTime:   time.Nanosecond,
	})
	got, err := ex.MapSliceBatch(context.Background(), data, func(b []int) []int { return b })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("len = %d, want %d", len(got), n)
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("result[%d] = %d, want %d (batch order broken)", i, v, i)
		}
	}
}

// TestAdaptiveOptions runs through the option surface.
func TestAdaptiveOptions(t *testing.T) {
	var count atomic.Int64
	err := ForEach(make([]int, 10_000), func(int) { count.Add(1) },
		MaxWorkers(4),
		MorselSize(16),
		AdaptiveMorselSize(true),
		MinMorselSize(16),
		MaxMorselSize(1024),
		TargetMorselTime(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 10_000 {
		t.Fatalf("count = %d, want 10000", count.Load())
	}
}
