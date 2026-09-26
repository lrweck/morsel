package morsel

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// busy spins for roughly d. It backs the synthetic per-element costs below.
func busy(d time.Duration) {
	if d <= 0 {
		return
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
	}
}

// adaptiveBenchCase is one synthetic callback cost.
type adaptiveBenchCase struct {
	name string
	n    int
	// cost is the per-element spin; skewSlow adds a much slower element every
	// skewEvery elements when non-zero.
	cost     time.Duration
	skewSlow time.Duration
}

var adaptiveBenchCases = []adaptiveBenchCase{
	{name: "trivial", n: 1 << 18},
	{name: "1us", n: 20_000, cost: time.Microsecond},
	{name: "10us", n: 2_000, cost: 10 * time.Microsecond},
	{name: "100us", n: 200, cost: 100 * time.Microsecond},
	{name: "1ms", n: 50, cost: time.Millisecond},
	{name: "skewed", n: 20_000, skewSlow: time.Millisecond},
}

// BenchmarkAdaptiveMorsel compares fixed morsel sizes against adaptive sizing
// across synthetic callback costs. It reports morsels per pass and the steal
// success rate in addition to ns/op and allocations.
func BenchmarkAdaptiveMorsel(b *testing.B) {
	for _, tc := range adaptiveBenchCases {
		data := make([]int, tc.n)
		for i := range data {
			data[i] = i
		}
		fn := func(v int) {
			if tc.cost > 0 {
				busy(tc.cost)
			}
			if tc.skewSlow > 0 && v%1000 == 0 {
				busy(tc.skewSlow)
			}
		}

		for _, size := range []uint{64, 256, 1024, 4096} {
			b.Run(tc.name+"/fixed-"+strconv.FormatUint(uint64(size), 10), func(b *testing.B) {
				benchAdaptiveRun(b, adaptiveBenchRunConfig(size, false), data, fn)
			})
		}
		b.Run(tc.name+"/adaptive", func(b *testing.B) {
			benchAdaptiveRun(b, adaptiveBenchRunConfig(256, true), data, fn)
		})
	}
}

// BenchmarkAdaptiveMorselScaled is the same comparison on a fixed input, to
// show throughput across worker counts for a mid-cost callback.
func BenchmarkAdaptiveMorselScaled(b *testing.B) {
	data := make([]int, 1<<18)
	for i := range data {
		data[i] = i
	}
	fn := func(int) { busy(time.Microsecond) }
	workers := []uint{1, 2, 4, 8, 16}
	for _, w := range workers {
		cfg := adaptiveBenchRunConfig(256, true)
		cfg.MaxWorkers = w
		b.Run("workers-"+strconv.FormatUint(uint64(w), 10)+"/adaptive", func(b *testing.B) {
			benchAdaptiveRun(b, cfg, data, fn)
		})
		cfgFixed := adaptiveBenchRunConfig(256, false)
		cfgFixed.MaxWorkers = w
		b.Run("workers-"+strconv.FormatUint(uint64(w), 10)+"/fixed-256", func(b *testing.B) {
			benchAdaptiveRun(b, cfgFixed, data, fn)
		})
	}
}

func adaptiveBenchRunConfig(size uint, adaptive bool) Config {
	cfg := Config{
		MaxWorkers:    uint(min(runtime.GOMAXPROCS(0), 8)),
		MorselSize:    size,
		QueueCapacity: 32,
		StealAttempts: 4,
	}
	if adaptive {
		cfg.AdaptiveMorselSize = true
		cfg.MinMorselSize = 64
		cfg.MaxMorselSize = 4096
		cfg.TargetMorselTime = time.Millisecond
	}
	return cfg
}

func benchAdaptiveRun(b *testing.B, cfg Config, data []int, fn func(int)) {
	ex := NewExecutor(cfg)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := ex.ForEachSlice(context.Background(), data, func(v int) error {
			fn(v)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	stats := ex.Stats()
	b.ReportMetric(float64(stats.MorselsExecuted)/float64(b.N), "morsels/pass")
	if stats.StealsAttempted > 0 {
		b.ReportMetric(100*float64(stats.StealsSucceeded)/float64(stats.StealsAttempted), "steal-%")
	} else {
		b.ReportMetric(0, "steal-%")
	}
}
