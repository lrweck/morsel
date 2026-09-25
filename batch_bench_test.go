package morsel

import (
	"context"
	"iter"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func benchSeqData() iter.Seq[int] {
	return func(yield func(int) bool) {
		for _, v := range benchData {
			if !yield(v) {
				return
			}
		}
	}
}

// BenchmarkSeqReduce is the iterator-path control for the Eager variant:
// same heavy reduction, producer batches to MorselSize as usual.
func BenchmarkSeqReduce(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
	})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := ex.ReduceSeq(context.Background(), benchSeqData(), 0,
			func(acc, v int) int { return acc + benchWork(v) },
			func(a, c int) int { return a + c })
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// BenchmarkSeqReduceEager is BenchmarkSeqReduce with Eager: steady-state
// framing is unchanged (one single-item first morsel per 1M items), so bulk
// throughput must match.
func BenchmarkSeqReduceEager(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
		Eager: true,
	})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := ex.ReduceSeq(context.Background(), benchSeqData(), 0,
			func(acc, v int) int { return acc + benchWork(v) },
			func(a, c int) int { return a + c })
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// BenchmarkBatchReduceHeavy is BenchmarkMorsel with the fold amortized over
// the whole batch: one callback call per morsel instead of per element.
func BenchmarkBatchReduceHeavy(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
	})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := ex.ReduceSliceBatch(context.Background(), benchData, 0,
			func(acc int, batch []int) int {
				for _, v := range batch {
					acc += benchWork(v)
				}
				return acc
			},
			func(a, c int) int { return a + c })
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// BenchmarkBatchLight is BenchmarkLight/16 with the trivial fold amortized
// over the batch: the per-element callback overhead is what keeps Light
// behind the plain loop, and batching removes most of it.
func BenchmarkBatchLight(b *testing.B) {
	ex := NewExecutor(Config{MaxWorkers: 16, MorselSize: 256, StealAttempts: 4})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := ex.ReduceSliceBatch(context.Background(), benchData, 0,
			func(acc int, batch []int) int {
				for _, v := range batch {
					acc += v
				}
				return acc
			},
			func(a, c int) int { return a + c })
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// Streaming benchmarks: a paced producer (one item per 50µs, 256 items)
// models a slow stream. Total time is producer-bound for every approach, so
// ns/op ties; what differs is first-ns/op, the time to the FIRST processed
// item. Defaults (GOMAXPROCS, MorselSize 256) plus one flag for eager.
const (
	streamItems = 256
	streamPace  = 50 * time.Microsecond
)

func streamWork(v int) int {
	x := v
	for range 8 {
		x = x*1664525 + 1013904223
		x ^= x >> 13
	}
	return x
}

func reportFirst(b *testing.B, sum int64) {
	b.ReportMetric(float64(sum)/float64(b.N), "first-ns/op")
}

// BenchmarkStreamBatched buffers the whole paced stream into one morsel:
// the first result waits for the last arrival.
func BenchmarkStreamBatched(b *testing.B) {
	b.ReportAllocs()
	var firstSum int64
	for b.Loop() {
		start := time.Now()
		var first atomic.Int64
		i := 0
		err := ForEachSeq(func(yield func(int) bool) {
			for ; i < streamItems; i++ {
				if !yield(i) {
					return
				}
				time.Sleep(streamPace)
			}
		}, func(v int) {
			first.CompareAndSwap(0, int64(time.Since(start)))
			_ = streamWork(v)
		}, MorselSize(256))
		if err != nil {
			b.Fatal(err)
		}
		firstSum += first.Load()
	}
	reportFirst(b, firstSum)
}

// BenchmarkStreamEager is BenchmarkStreamBatched with Eager: the first line
// goes immediately, the rest follows the stream.
func BenchmarkStreamEager(b *testing.B) {
	b.ReportAllocs()
	var firstSum int64
	for b.Loop() {
		start := time.Now()
		var first atomic.Int64
		i := 0
		err := ForEachSeq(func(yield func(int) bool) {
			for ; i < streamItems; i++ {
				if !yield(i) {
					return
				}
				time.Sleep(streamPace)
			}
		}, func(v int) {
			first.CompareAndSwap(0, int64(time.Since(start)))
			_ = streamWork(v)
		}, MorselSize(256), Eager(true))
		if err != nil {
			b.Fatal(err)
		}
		firstSum += first.Load()
	}
	reportFirst(b, firstSum)
}

// BenchmarkStreamChannelPool is the traditional answer: a fixed pool fed by
// a per-item channel. Immediate first item, at the cost of per-item channel
// ops and a hard-coded worker count.
func BenchmarkStreamChannelPool(b *testing.B) {
	b.ReportAllocs()
	var firstSum int64
	for b.Loop() {
		start := time.Now()
		var first atomic.Int64
		jobs := make(chan int)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for v := range jobs {
					first.CompareAndSwap(0, int64(time.Since(start)))
					_ = streamWork(v)
				}
			})
		}
		for i := range streamItems {
			jobs <- i
			time.Sleep(streamPace)
		}
		close(jobs)
		wg.Wait()
		firstSum += first.Load()
	}
	reportFirst(b, firstSum)
}

// BenchmarkStreamSequential is the floor: a plain loop over the paced
// stream. Immediate first item, zero parallelism.
func BenchmarkStreamSequential(b *testing.B) {
	b.ReportAllocs()
	var firstSum int64
	for b.Loop() {
		start := time.Now()
		var first int64
		for i := range streamItems {
			if first == 0 {
				first = int64(time.Since(start))
			}
			_ = streamWork(i)
			time.Sleep(streamPace)
		}
		firstSum += first
	}
	reportFirst(b, firstSum)
}
