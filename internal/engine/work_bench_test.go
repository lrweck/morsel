package engine

import (
	"context"
	"testing"
	"unsafe"

	"github.com/lrweck/morsel/internal/queue"
)

// TestWorkSize records the in-memory size of a Work value. Work is passed by
// value through the worker queues, so its size is a direct multiplier on the
// queue's cache footprint.
func TestWorkSize(t *testing.T) {
	t.Logf("sizeof(Work[int]) = %d bytes", unsafe.Sizeof(Work[int]{}))
}

// workBenchItems is the number of elements a benchmark morsel refers to. The
// element slice itself is allocated once and shared, so the measured cost is
// the Work header, not the data.
const workBenchItems = 256

func benchWorkValue(i int) Work[int] {
	return Work[int]{Start: i, Items: make([]int, workBenchItems)}
}

// BenchmarkWorkQueuePushPop measures one publication plus one consumption of a
// Work[int] through the bounded SPMC queue that backs the scheduler.
func BenchmarkWorkQueuePushPop(b *testing.B) {
	q := queue.NewSPMC[Work[int]](1024)
	w := benchWorkValue(0)
	b.ReportAllocs()
	for b.Loop() {
		q.Push(w)
		q.Pop()
	}
}

// BenchmarkWorkQueueBurst fills the queue to capacity and then drains it. With
// many Work values live at once the per-item size fixes the queue's cache
// footprint, which is where an added field would show up as a throughput cost.
func BenchmarkWorkQueueBurst(b *testing.B) {
	const depth = 64
	q := queue.NewSPMC[Work[int]](depth)
	items := make([]Work[int], depth)
	for i := range items {
		items[i] = benchWorkValue(i)
	}
	b.ReportAllocs()
	for b.Loop() {
		for i := range items {
			q.Push(items[i])
		}
		for range items {
			q.Pop()
		}
	}
}

// BenchmarkRunSliceTinyMorsel drives the real scheduler with one-element
// morsels over a small input, so publication and consumption dominate and the
// per-Work size shows up end to end.
func BenchmarkRunSliceTinyMorsel(b *testing.B) {
	data := make([]int, 1024)
	cfg := Config{MaxWorkers: 8, MorselSize: 1, QueueCapacity: 64, StealAttempts: 4}
	process := func(_ *Worker[int, struct{}], _ Work[int]) error { return nil }
	newState := func() struct{} { return struct{}{} }
	merge := func(*struct{}, struct{}) {}
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := RunSlice(cfg, context.Background(), data, process, newState, merge); err != nil {
			b.Fatal(err)
		}
	}
}
