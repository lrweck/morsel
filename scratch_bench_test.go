package morsel

import (
	"context"
	"sync"
	"testing"

	"github.com/lrweck/morsel/internal/engine"
)

// Issue #6 — "Benchmark worker-local scratch buffers against sync.Pool".
//
// Decision: DO NOT ADOPT worker-local scratch. Measurements (GOOS=linux,
// GOARCH=amd64, i7-13700H, GOMAXPROCS=20, median of 5):
//
//   - The pool itself costs ~9-10 ns per morsel (BenchmarkScratchMechanism/
//     pool-get-put), against ~900 ns per morsel for the real single-worker
//     Map->ForEach pipeline (BenchmarkScratchMapForEach/1, 3.6-3.8 ms / 4096
//     morsels). It is ~1% of the per-morsel cost; the pipeline's own two
//     `emit` closures per morsel dominate (8216 allocs / 4096 morsels).
//   - In the like-for-like engine harness (BenchmarkScratchHarness) the
//     theoretical ceiling — a typed buffer owned by Worker.State, with no
//     side table and no per-morsel type assertion — is faster at 1 worker
//     (539-592 µs vs 663-732 µs, i.e. 10-26% across two `-count=5` runs)
//     and within noise (or slower) at 2/4/8/16 workers. The 1-worker figure
//     is the noisiest in the suite: the isolated pool-cycle/worker-cycle
//     benchmarks disagree between runs for the same code.
//   - The only non-intrusive implementation (sidemap: sync.Map keyed by the
//     worker pointer + a per-morsel `any` assertion) is never faster than the
//     pool and adds 2 allocs per run.
//
// A worker-local design therefore needs intrusive engine plumbing (a typed
// sidecar threaded through Pipeline.apply) to buy a ~10 ns/morsel win that is
// invisible at the worker counts where the pool actually matters. It also
// costs allocations: Worker.State is zeroed on runner release and the pipeline
// builds a fresh Runner per run, so scratch would be reallocated per run.
//
// The benchmarks stay as the evidence. If the engine ever threads a typed
// per-stage sidecar for free, rerun BenchmarkScratchHarness before adopting.
//
// maxRetainedScratch is the retention cap proposed by issue #6. A worker-local
// buffer whose capacity exceeds it is dropped after use so it can be GC'd.
const maxRetainedScratch = 64 << 10

// ---------------------------------------------------------------------------
// Required end-to-end measurement: the current stage-local sync.Pool, through
// the real fluent pipeline.
// ---------------------------------------------------------------------------

// BenchmarkScratchMapForEach is one Map stage over a large slice, consumed by
// ForEach. MorselSize 256 over benchSize (1<<20) is 4096 morsels.
func BenchmarkScratchMapForEach(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(sizeName(workers), func(b *testing.B) {
			ex := NewExecutor(Config{MaxWorkers: uint(workers), MorselSize: 256})
			b.ReportAllocs()
			for b.Loop() {
				if err := Slice(benchData).
					Map(func(v int) int { return v + 1 }).
					ForEach(func(int) {}, WithExecutor(ex)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkScratchMapFilterMapForEach is the four-stage shape from issue #6:
// two Map scratch pools plus one Filter pool per run.
func BenchmarkScratchMapFilterMapForEach(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(sizeName(workers), func(b *testing.B) {
			ex := NewExecutor(Config{MaxWorkers: uint(workers), MorselSize: 256})
			b.ReportAllocs()
			for b.Loop() {
				if err := Slice(benchData).
					Map(func(v int) int { return v + 1 }).
					Filter(func(v int) bool { return v&1 == 0 }).
					Map(func(v int) int { return v * 2 }).
					ForEach(func(int) {}, WithExecutor(ex)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Like-for-like prototype harness.
//
// All variants run the exact same engine (engine.RunSlice), the same morsels,
// the same transform and the same synchronous consume, and differ only in how
// the per-morsel `[]int` buffer is obtained:
//
//   - pool:         stage-local sync.Pool, i.e. the current production code.
//   - worker:       typed buffer owned by Worker.State — the theoretical
//                   ceiling of a worker-local design, with no side table and
//                   no per-morsel type assertion. NOT implementable without
//                   an engine change that threads a typed sidecar.
//   - workerCapped: worker-local, dropping the buffer when cap exceeds
//                   maxRetainedScratch.
//   - sidemap:      sync.Map keyed by worker pointer with an `any` value and a
//                   per-morsel type assertion — the non-intrusive fallback the
//                   issue warns against.
// ---------------------------------------------------------------------------

// harnessState is the terminal accumulator shared by every variant: buf is the
// worker-owned scratch (worker-local variants only) and sum keeps the consumed
// output live so the compiler cannot elide the loop.
type harnessState struct {
	buf []int
	sum int64
}

func newHarnessState() harnessState                    { return harnessState{} }
func mergeHarness(dst *harnessState, src harnessState) { dst.sum += src.sum }

func harnessConfig(workers int) engine.Config {
	return engine.Config{MaxWorkers: uint(workers), MorselSize: 256, QueueCapacity: 32, StealAttempts: 4}
}

func harnessRun(b *testing.B, cfg engine.Config, process func(*engine.Worker[int, harnessState], engine.Work[int]) error) {
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := engine.RunSlice(cfg, context.Background(), benchData, process, newHarnessState, mergeHarness); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkScratchHarness(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		cfg := harnessConfig(workers)

		b.Run("pool/"+sizeName(workers), func(b *testing.B) {
			scr := newScratch[int]() // exactly the production Map scratch
			process := func(w *engine.Worker[int, harnessState], m engine.Work[int]) error {
				bp := scr.get()
				buf := (*bp)[:0]
				for _, v := range m.Items {
					buf = append(buf, v+1)
				}
				var s int64
				for _, v := range buf {
					s += int64(v)
				}
				w.State.sum += s
				*bp = buf
				scr.put(bp)
				return nil
			}
			harnessRun(b, cfg, process)
		})

		b.Run("worker/"+sizeName(workers), func(b *testing.B) {
			process := func(w *engine.Worker[int, harnessState], m engine.Work[int]) error {
				buf := w.State.buf[:0]
				for _, v := range m.Items {
					buf = append(buf, v+1)
				}
				var s int64
				for _, v := range buf {
					s += int64(v)
				}
				w.State.sum += s
				w.State.buf = buf
				return nil
			}
			harnessRun(b, cfg, process)
		})

		b.Run("workerCapped/"+sizeName(workers), func(b *testing.B) {
			process := func(w *engine.Worker[int, harnessState], m engine.Work[int]) error {
				buf := w.State.buf[:0]
				for _, v := range m.Items {
					buf = append(buf, v+1)
				}
				var s int64
				for _, v := range buf {
					s += int64(v)
				}
				w.State.sum += s
				if cap(buf) > maxRetainedScratch {
					buf = nil
				}
				w.State.buf = buf
				return nil
			}
			harnessRun(b, cfg, process)
		})

		b.Run("sidemap/"+sizeName(workers), func(b *testing.B) {
			var table sync.Map // *engine.Worker[int,harnessState] -> *[]int
			process := func(w *engine.Worker[int, harnessState], m engine.Work[int]) error {
				v, ok := table.Load(w)
				if !ok {
					v = new([]int)
					table.Store(w, v)
				}
				bp := v.(*[]int) // per-morsel type assertion
				buf := (*bp)[:0]
				for _, v := range m.Items {
					buf = append(buf, v+1)
				}
				var s int64
				for _, v := range buf {
					s += int64(v)
				}
				w.State.sum += s
				*bp = buf
				return nil
			}
			harnessRun(b, cfg, process)
		})
	}
}

// BenchmarkScratchMechanism isolates the raw per-morsel cost of the pool
// against plain local reuse, single-goroutine, so the numbers are not diluted
// by the engine.
func BenchmarkScratchMechanism(b *testing.B) {
	const n = 256

	b.Run("pool-get-put", func(b *testing.B) {
		s := newScratch[int]()
		bp := s.get()
		*bp = make([]int, n)
		s.put(bp)
		b.ReportAllocs()
		for b.Loop() {
			s.put(s.get())
		}
	})

	b.Run("pool-cycle", func(b *testing.B) {
		s := newScratch[int]()
		b.ReportAllocs()
		for b.Loop() {
			bp := s.get()
			buf := (*bp)[:0]
			for i := range n {
				buf = append(buf, i)
			}
			var sum int
			for _, v := range buf {
				sum += v
			}
			_ = sum
			*bp = buf
			s.put(bp)
		}
	})

	b.Run("worker-cycle", func(b *testing.B) {
		var held []int
		b.ReportAllocs()
		for b.Loop() {
			buf := held[:0]
			for i := range n {
				buf = append(buf, i)
			}
			var sum int
			for _, v := range buf {
				sum += v
			}
			_ = sum
			held = buf
		}
	})
}
