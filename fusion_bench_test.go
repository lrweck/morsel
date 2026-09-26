package morsel

import (
	"context"
	"runtime"
	"testing"
)

// Issue #3 investigation: should a chain of Map/Filter stages be fused into a
// single pass? This file is the decision record. It ships only benchmarks and
// a reference; pipeline.go is unchanged.
//
// DECISION: do NOT implement stage fusion.
//
// What the pipeline actually costs. The pipeline is built from closures: each
// Map/Filter wraps the downstream emit in a *fresh* closure on every morsel,
// so an N-stage chain allocates N+1 closures per morsel (one per transform
// plus the terminal), independent of morsel size. The scratch buffers are
// pooled and reused — the top-of-file claim in pipeline.go that a stage
// "allocates nothing per morsel" is wrong; what it allocates is the emit
// closure, not the buffer. A 1M-element slice at MorselSize 256 is 4096
// morsels, hence the flat 8217/12327/16438/20549 allocs/op for 1..4 stages.
// Hand-written fused loops allocate 0.
//
// Measured gap (median of 7 runs at -benchtime=500ms, shared 20-core box;
// fused references call the user funcs through opaque globals so neither side
// can be devirtualized). Light ops are a multiply/mask chain; heavy ops are
// the 64-round hash in benchmark_test.go, i.e. a realistic validate/calculate:
//
//	chain (sequential, MaxWorkers=1)   pipeline   fused    speedup
//	1 Map                                 3.69ms   1.15ms   3.2x
//	2 Map->Filter                         4.80ms   2.32ms   2.1x
//	3 Map->Filter->Map                    6.20ms   3.10ms   2.0x
//	4 Map->Filter->Map->Filter            8.15ms   4.05ms   2.0x
//	1 Map (heavy)                        58.2ms   54.0ms    1.08x
//	2 Map->Filter (heavy)                60.1ms   55.2ms    1.09x
//	3 Map->Filter->Map (heavy)           86.8ms  104.9ms    0.83x
//
//	parallel, GOMAXPROCS=20 (median of 10 runs)
//	4 Map->Filter->Map->Filter (light)   1.31ms   0.85ms   1.5x
//	3 Map->Filter->Map (heavy)           11.0ms   11.3ms   0.97x
//
// Why not fuse. Fusion is a 2x win only on the single-worker path with trivial
// element functions — the case where per-morsel overhead dominates. Add any
// real per-element work and the win collapses to ~8% (1-2 stages) or turns
// into a loss: a fused loop keeps a loop-carried accumulator and serializes
// the conditional second map, losing the instruction-level parallelism the
// separate passes get, so the 3-stage heavy case is 17-21% *slower*. On the
// parallel path, the realistic operating mode, the best observed win is 1.5x
// for the cheapest possible ops, and with realistic ops fused is 3% slower
// than the pipeline. The saved work is closure allocation, which is per
// morsel, not per element, so parallelism already amortizes it.
//
// A fusible-shape rewrite would also only help the four hard-coded shapes; the
// per-morsel closure cost is common to every chain, fusible or not. The real
// lever is eliminating the per-morsel emit closures themselves (e.g. build the
// emit chain once per worker instead of once per morsel), which is a smaller,
// shape-agnostic change — out of scope for issue #3.
//
// The fused references below call the user functions through package-level
// function variables, the same indirection a real fusion executor would have
// (user funcs are data it cannot devirtualize), so the comparison is
// like-for-like.

// sink keeps results observable so the compiler cannot eliminate the loops.
var sink int

// Light ops: structural overhead is a large fraction of the total, fusion's
// best case.
func fusM1(v int) int  { return v*3 + 1 }
func fusP1(v int) bool { return v&3 != 0 } // keep 3/4
func fusM2(v int) int  { return v ^ (v >> 7) }
func fusP2(v int) bool { return v%5 != 0 } // keep 4/5

// Heavy ops: the per-element cost a real validate/calculate step has.
func heavyM(v int) int  { return benchWork(v) }
func heavyP(v int) bool { return v&1 == 0 }

// Opaque function values: globals are loaded at run time, so neither the
// pipeline nor the fused loops can inline/devirtualize the user functions.
// Both sides pay the same indirect call.
var (
	bm1 func(int) int  = fusM1
	bp1 func(int) bool = fusP1
	bm2 func(int) int  = fusM2
	bp2 func(int) bool = fusP2
	bhm func(int) int  = heavyM
	bhp func(int) bool = heavyP
)

// Fused references: one pass, same opaque funcs. This is the ceiling a
// specialized executor could reach without changing the public API.
func fused1(data []int, m1 func(int) int) int {
	sum := 0
	for _, v := range data {
		sum += m1(v)
	}
	return sum
}

func fused2(data []int, m1 func(int) int, p1 func(int) bool) int {
	sum := 0
	for _, v := range data {
		if a := m1(v); p1(a) {
			sum += a
		}
	}
	return sum
}

func fused3(data []int, m1 func(int) int, p1 func(int) bool, m2 func(int) int) int {
	sum := 0
	for _, v := range data {
		if a := m1(v); p1(a) {
			sum += m2(a)
		}
	}
	return sum
}

func fused4(data []int, m1 func(int) int, p1 func(int) bool, m2 func(int) int, p2 func(int) bool) int {
	sum := 0
	for _, v := range data {
		if a := m1(v); p1(a) {
			if b := m2(a); p2(b) {
				sum += b
			}
		}
	}
	return sum
}

// TestFusionReferenceMatchesPipeline pins the fused references to the pipeline
// so the benchmark comparison is like-for-like.
func TestFusionReferenceMatchesPipeline(t *testing.T) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	opt := []Option{MaxWorkers(1), MorselSize(64)}

	var got int
	if err := Slice(data).Map(fusM1).ForEach(func(v int) { got += v }, opt...); err != nil {
		t.Fatal(err)
	}
	if want := fused1(data, fusM1); got != want {
		t.Fatalf("1-stage: pipe=%d fused=%d", got, want)
	}

	got = 0
	if err := Slice(data).Map(fusM1).Filter(fusP1).ForEach(func(v int) { got += v }, opt...); err != nil {
		t.Fatal(err)
	}
	if want := fused2(data, fusM1, fusP1); got != want {
		t.Fatalf("2-stage: pipe=%d fused=%d", got, want)
	}

	got = 0
	if err := Slice(data).Map(fusM1).Filter(fusP1).Map(fusM2).ForEach(func(v int) { got += v }, opt...); err != nil {
		t.Fatal(err)
	}
	if want := fused3(data, fusM1, fusP1, fusM2); got != want {
		t.Fatalf("3-stage: pipe=%d fused=%d", got, want)
	}

	got = 0
	if err := Slice(data).Map(fusM1).Filter(fusP1).Map(fusM2).Filter(fusP2).ForEach(func(v int) { got += v }, opt...); err != nil {
		t.Fatal(err)
	}
	if want := fused4(data, fusM1, fusP1, fusM2, fusP2); got != want {
		t.Fatalf("4-stage: pipe=%d fused=%d", got, want)
	}
}

// seqOpt forces the engine's single-goroutine path, isolating CPU structure
// from parallel scheduling noise.
func seqOpt() []Option { return []Option{MaxWorkers(1), MorselSize(256)} }

// BenchmarkFusionPipelineLight is the current nested apply/emit path on the
// sequential fast path for 1..4 light stages.
func BenchmarkFusionPipelineLight(b *testing.B) {
	consume := func(int) {}
	b.Run("1-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bm1).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("2-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bm1).Filter(bp1).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("3-map-filter-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bm1).Filter(bp1).Map(bm2).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("4-map-filter-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bm1).Filter(bp1).Map(bm2).Filter(bp2).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("4-map-filter-map-filter-reduce", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := Slice(benchData).Map(bm1).Filter(bp1).Map(bm2).Filter(bp2).
				Reduce(0, func(acc, v int) int { return acc + v }, func(a, c int) int { return a + c }, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFusionFusedLight is the like-for-like reference for the light ops.
func BenchmarkFusionFusedLight(b *testing.B) {
	b.Run("1-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused1(benchData, bm1)
		}
	})
	b.Run("2-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused2(benchData, bm1, bp1)
		}
	})
	b.Run("3-map-filter-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused3(benchData, bm1, bp1, bm2)
		}
	})
	b.Run("4-map-filter-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused4(benchData, bm1, bp1, bm2, bp2)
		}
	})
}

// BenchmarkFusionPipelineHeavy repeats 3 stages with a 64-round hash per map.
func BenchmarkFusionPipelineHeavy(b *testing.B) {
	consume := func(int) {}
	b.Run("1-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bhm).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("2-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bhm).Filter(bhp).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("3-map-filter-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := Slice(benchData).Map(bhm).Filter(bhp).Map(bhm).ForEach(consume, seqOpt()...); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkFusionFusedHeavy is the fused reference for the heavy ops.
func BenchmarkFusionFusedHeavy(b *testing.B) {
	b.Run("1-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused1(benchData, bhm)
		}
	})
	b.Run("2-map-filter", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused2(benchData, bhm, bhp)
		}
	})
	b.Run("3-map-filter-map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sink = fused3(benchData, bhm, bhp, bhm)
		}
	})
}

// BenchmarkFusionPooled compares the full engine (morsels in parallel) using
// the pipeline vs a single fused pass per morsel. ForEachSliceBatch goes
// through the same engine runner as the pipeline; the fused pass replaces the
// whole apply chain with one loop.
func BenchmarkFusionPooled(b *testing.B) {
	ex := NewExecutor(Config{
		MaxWorkers: uint(runtime.GOMAXPROCS(0)), MorselSize: 256, StealAttempts: 4,
	})
	consume := func(int) {}
	b.Run("light", func(b *testing.B) {
		b.Run("pipeline", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := Slice(benchData).Map(bm1).Filter(bp1).Map(bm2).Filter(bp2).
					ForEach(consume, WithExecutor(ex)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("fused-batch", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				// Accumulate per morsel, not per element, so workers do not
				// contend on a shared variable.
				if err := ex.ForEachSliceBatch(context.Background(), benchData, func(items []int) error {
					sum := 0
					for _, v := range items {
						if a := bm1(v); bp1(a) {
							if b := bm2(a); bp2(b) {
								sum += b
							}
						}
					}
					sink = sum
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("heavy", func(b *testing.B) {
		b.Run("pipeline", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := Slice(benchData).Map(bhm).Filter(bhp).Map(bhm).
					ForEach(consume, WithExecutor(ex)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("fused-batch", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := ex.ForEachSliceBatch(context.Background(), benchData, func(items []int) error {
					sum := 0
					for _, v := range items {
						if a := bhm(v); bhp(a) {
							sum += bhm(a)
						}
					}
					sink = sum
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}
