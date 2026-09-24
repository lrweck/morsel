package morsel

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

const benchSize = 1 << 20

var benchData = func() []int {
	data := make([]int, benchSize)
	for i := range data {
		data[i] = i
	}
	return data
}()

func benchWork(v int) int {
	x := v
	for range 64 {
		x = x*1664525 + 1013904223
		x ^= x >> 13
	}
	return x
}

// BenchmarkSequential is the baseline single-threaded loop.
func BenchmarkBaselineLoop(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sum := 0
		for _, v := range benchData {
			sum += benchWork(v)
		}
		_ = sum
	}
}

// BenchmarkWorkerPoolChannel is a traditional fixed pool fed by a channel, for
// comparison with the morsel engine.
func BenchmarkChannelPool(b *testing.B) {
	const workers = 8
	const chunk = 256
	b.ReportAllocs()
	for b.Loop() {
		jobs := make(chan [2]int, workers*2)
		var wg sync.WaitGroup
		var mu sync.Mutex
		sum := 0
		for range workers {
			wg.Go(func() {
				local := 0
				for j := range jobs {
					for k := j[0]; k < j[1]; k++ {
						local += benchWork(benchData[k])
					}
				}
				mu.Lock()
				sum += local
				mu.Unlock()
			})
		}
		for start := 0; start < len(benchData); start += chunk {
			jobs <- [2]int{start, min(start+chunk, len(benchData))}
		}
		close(jobs)
		wg.Wait()
		_ = sum
	}
}

// BenchmarkMorsel is the morsel engine doing the same reduction.
func BenchmarkMorsel(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
	})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := ex.ReduceSlice(context.Background(), benchData, 0,
			func(acc, v int) int { return acc + benchWork(v) },
			func(a, c int) int { return a + c })
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// BenchmarkForEachSliceAllocs varies the input size. If allocs/op stays flat
// while the input grows 1000x, then nothing is allocated per morsel: the cost
// is the per-run worker queues only, and the morsel path is pure CPU.
func BenchmarkForEachSliceAllocs(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: 8, MorselSize: 256, StealAttempts: 4,
	})
	for _, size := range []int{1_000, 100_000, 1_000_000} {
		data := make([]int, size)
		b.Run(sizeName(size), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := ex.ForEachSlice(context.Background(), data, func(int) error {
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkPipelineMapReduce measures the fluent path, which materializes one
// intermediate morsel per Map stage, so it does allocate per morsel. The
// primitives (ForEachSlice/MapSlice/ReduceSlice) are the allocation-free path.
func BenchmarkPipelineMapReduce(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
	})
	b.ReportAllocs()
	for b.Loop() {
		sum, err := Slice(benchData).
			Map(benchWork).
			Reduce(0, func(acc, v int) int { return acc + v }, func(a, c int) int { return a + c },
				WithExecutor(ex))
		if err != nil {
			b.Fatal(err)
		}
		_ = sum
	}
}

// BenchmarkHandshakeHeavy forces constant producer/worker handshakes: a tiny
// queue and one element per morsel, so park/wake dominates.
func BenchmarkHandshakeHeavy(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: 8, MorselSize: 1, QueueCapacity: 2, StealAttempts: 4,
	})
	data := make([]int, 50_000)
	b.ReportAllocs()
	for b.Loop() {
		if err := ex.ForEachSlice(context.Background(), data, func(int) error {
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMorselSizes(b *testing.B) {
	for _, size := range []int{32, 64, 128, 256, 512, 1024, 4096} {
		b.Run(sizeName(size), func(b *testing.B) {
			ex := NewExecutor(Config{
				MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: uint(size), StealAttempts: 4,
			})
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ex.ReduceSlice(context.Background(), benchData, 0,
					func(acc, v int) int { return acc + benchWork(v) },
					func(a, c int) int { return a + c }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMorselWorkers(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(sizeName(workers), func(b *testing.B) {
			ex := NewExecutor(Config{
				MaxWorkers: uint(workers), MorselSize: 256, StealAttempts: 4,
			})
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ex.ReduceSlice(context.Background(), benchData, 0,
					func(acc, v int) int { return acc + benchWork(v) },
					func(a, c int) int { return a + c }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMorselIrregular makes the per-element cost depend on the value, so
// work stealing has something to balance.
func BenchmarkMorselIrregular(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 64, StealAttempts: 8,
	})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ex.ReduceSlice(context.Background(), benchData, 0,
			func(acc, v int) int {
				for j := 0; j < v%7; j++ {
					acc += j
				}
				return acc + v
			},
			func(a, c int) int { return a + c }); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSmall is a tiny input that fits in one morsel, so it runs on the
// caller (no pool), against the same input forced through the pool.
func BenchmarkTinyInput(b *testing.B) {
	data := make([]int, 100)
	b.Run("sequential", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ForEach(data, func(int) {}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ForEach(data, func(int) {}, MorselSize(1), MaxWorkers(4)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkLight is a lot of trivial work: the per-morsel overhead does not
// vanish with parallelism, so it shows whether more workers pay off at all.
// workers=1 runs on the caller thanks to the sequential fast path.
func BenchmarkLight(b *testing.B) {
	b.Run("baseline", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sum := 0
			for _, v := range benchData {
				sum += v
			}
			_ = sum
		}
	})
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(sizeName(workers), func(b *testing.B) {
			ex := NewExecutor(Config{
				MaxWorkers: uint(workers), MorselSize: 256, StealAttempts: 4,
			})
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ex.ReduceSlice(context.Background(), benchData, 0,
					func(acc, v int) int { return acc + v },
					func(a, c int) int { return a + c }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func sizeName(n int) string { return strconv.Itoa(n) }
