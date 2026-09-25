package morsel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// Slow-rate validation for Eager.
//
// Regime is defined by arrival rate vs TOTAL capacity (workers x per-line
// cost), not by one worker: with the default pool the arrival below never
// backlogs, and immediate singles are the correct answer. So both cases run
// MaxWorkers(2): overloaded arrives at 200 lines/s against 160 lines/s of
// capacity (backlog persists, morsels must fill toward MorselSize after
// warm-up); starved arrives at the same 200/s against 1000 lines/s (arrivals
// find no backlog, morsels stay small). Without Eager every morsel is full
// but the first result waits for a full morsel.
//
// Timing-sensitive by nature; skipped with -short. Bounds are loose on
// purpose: scheduling jitter moves individual morsels between full and
// partial, but cannot flip either regime.
func TestEagerResponsiveVsBatched(t *testing.T) {
	// Responsiveness race favoring Eager: a slow producer (6 lines, 50ms
	// apart) against a MorselSize(256), so any batcher must wait for the
	// whole stream. Eager answers on the first line; the library without
	// Eager and a traditional collect-then-process program answer only at
	// the end. Totals tie (producer-bound); first-item latency does not.
	// Margins are wide (typical: ~1ms vs ~250ms); skipped with -short.
	if testing.Short() {
		t.Skip("timing-sensitive rate validation")
	}
	const (
		nlines     = 6
		pace       = 50 * time.Millisecond
		morselSize = 256
	)
	pacedPipe := func() io.Reader {
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			for i := range nlines {
				fmt.Fprintf(pw, "line-%d\n", i)
				time.Sleep(pace)
			}
		}()
		return pr
	}
	type outcome struct {
		first time.Duration
		total time.Duration
		got   []string
	}

	runLib := func(t *testing.T, eager bool) outcome {
		t.Helper()
		var out outcome
		start := time.Now()
		var firstNano atomic.Int64
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		o := []Option{MorselSize(morselSize), WithContext(ctx)}
		if eager {
			o = append(o, Eager(true))
		}
		err := Lines(pacedPipe()).ForEachBatch(func(b []string) {
			firstNano.CompareAndSwap(0, time.Now().UnixNano())
			out.got = append(out.got, b...)
		}, o...)
		if err != nil {
			t.Fatal(err)
		}
		out.first = time.Unix(0, firstNano.Load()).Sub(start)
		out.total = time.Since(start)
		return out
	}
	runTraditional := func() outcome {
		var out outcome
		start := time.Now()
		pr := pacedPipe()
		var all []string
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			all = append(all, sc.Text())
		}
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		for _, ln := range all {
			if out.first == 0 {
				out.first = time.Since(start)
			}
			out.got = append(out.got, ln)
		}
		out.total = time.Since(start)
		return out
	}

	eager := runLib(t, true)
	batched := runLib(t, false)
	trad := runTraditional()
	t.Logf("eager:       first=%v total=%v", eager.first, eager.total)
	t.Logf("lib-batched: first=%v total=%v", batched.first, batched.total)
	t.Logf("traditional: first=%v total=%v", trad.first, trad.total)

	for name, out := range map[string]outcome{"eager": eager, "batched": batched, "traditional": trad} {
		if len(out.got) != nlines {
			t.Fatalf("%s: got %d lines, want %d", name, len(out.got), nlines)
		}
		want := make([]string, nlines)
		for i := range nlines {
			want[i] = fmt.Sprintf("line-%d", i)
		}
		if !slices.Equal(slices.Sorted(slices.Values(out.got)), want) {
			t.Fatalf("%s: got %v", name, out.got)
		}
	}
	if eager.first >= 150*time.Millisecond {
		t.Fatalf("eager first = %v, want prompt (< 150ms)", eager.first)
	}
	for name, out := range map[string]outcome{"batched": batched, "traditional": trad} {
		if out.first <= 150*time.Millisecond {
			t.Fatalf("%s first = %v, want full-stream latency (> 150ms)", name, out.first)
		}
	}
}

func TestEagerFindFirstAbortsWholeRun(t *testing.T) {
	// Whole-run win, not just first-item: a poison pill at line 10 of a
	// 2000-line paced stream with MorselSize(256). Eager processes line 10
	// on arrival, returns the error and aborts everything after ~10 paces.
	// The library without Eager must wait for 256 arrivals to fill the
	// first morsel; a traditional collect-then-scan program reads all 2000
	// lines before looking. Totals — not first latencies — separate them.
	// Floors are bulletproof (sleeps never run short); the eager ceiling
	// has 10x margin. Skipped with -short.
	if testing.Short() {
		t.Skip("timing-sensitive rate validation")
	}
	const (
		nlines     = 2000
		poison     = 10
		pace       = 200 * time.Microsecond
		morselSize = 256
	)
	errPoison := errors.New("poison")
	pacedSeq := func(yield func(int) bool) {
		for i := range nlines {
			if !yield(i) {
				return
			}
			time.Sleep(pace)
		}
	}
	type outcome struct {
		total time.Duration
		calls int64
		err   error
	}

	runLib := func(t *testing.T, eager bool) outcome {
		t.Helper()
		var out outcome
		var calls atomic.Int64
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		o := []Option{MorselSize(morselSize), WithContext(ctx)}
		if eager {
			o = append(o, Eager(true))
		}
		out.err = ForEachSeqE(pacedSeq, func(v int) error {
			calls.Add(1)
			if v == poison {
				return errPoison
			}
			return nil
		}, o...)
		out.total = time.Since(start)
		out.calls = calls.Load()
		return out
	}
	runTraditional := func() outcome {
		var out outcome
		start := time.Now()
		var all []int
		pacedSeq(func(v int) bool { all = append(all, v); return true })
		for _, v := range all {
			out.calls++
			if v == poison {
				out.err = errPoison
				break
			}
		}
		out.total = time.Since(start)
		return out
	}

	eager := runLib(t, true)
	batched := runLib(t, false)
	trad := runTraditional()
	t.Logf("eager:       total=%v calls=%d err=%v", eager.total, eager.calls, eager.err)
	t.Logf("lib-batched: total=%v calls=%d err=%v", batched.total, batched.calls, batched.err)
	t.Logf("traditional: total=%v calls=%d err=%v", trad.total, trad.calls, trad.err)

	for name, out := range map[string]outcome{"eager": eager, "batched": batched, "traditional": trad} {
		if !errors.Is(out.err, errPoison) {
			t.Fatalf("%s: err = %v, want poison", name, out.err)
		}
	}
	// Deterministic shape: the batched run breaks inside its single first
	// morsel at index 10; the traditional scan breaks at the same place.
	if batched.calls != poison+1 || trad.calls != poison+1 {
		t.Fatalf("calls: batched=%d trad=%d, want %d", batched.calls, trad.calls, poison+1)
	}
	if eager.calls > 20 {
		t.Fatalf("eager calls = %d, want early abort (<= 20)", eager.calls)
	}
	// Bulletproof floors: 256 arrivals and 2000 arrivals cannot complete
	// faster than their paces on any machine.
	if batched.total <= 30*time.Millisecond {
		t.Fatalf("batched total = %v, want >= 256 paces", batched.total)
	}
	if trad.total <= 250*time.Millisecond {
		t.Fatalf("traditional total = %v, want the full span", trad.total)
	}
	if eager.total >= 30*time.Millisecond {
		t.Fatalf("eager total = %v, want ~10 paces", eager.total)
	}
	if !(eager.total < batched.total && batched.total < trad.total) {
		t.Fatalf("want eager < batched < traditional, got %v < %v < %v",
			eager.total, batched.total, trad.total)
	}
}

func TestEagerSlowRates(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive rate validation")
	}
	const (
		lines      = 240
		morselSize = 8
		writeEvery = 5 * time.Millisecond // 200 lines/s in
	)
	slowConsumer := 12500 * time.Microsecond // 80 lines/s/worker, 160 total
	fastConsumer := 2 * time.Millisecond     // 500 lines/s/worker, 1000 total

	type outcome struct {
		sizes []int
		mean  float64
		total int
	}
	run := func(t *testing.T, processEach time.Duration, eager bool) outcome {
		t.Helper()
		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			for i := range lines {
				fmt.Fprintf(pw, "line-%d\n", i)
				time.Sleep(writeEvery)
			}
		}()
		sizesCh := make(chan int, lines)
		opts := []Option{MorselSize(morselSize), MaxWorkers(2), QueueCapacity(4)}
		if eager {
			opts = append(opts, Eager(true))
		}
		err := Lines(pr).ForEachBatch(func(b []string) {
			sizesCh <- len(b)
			for range b {
				time.Sleep(processEach)
			}
		}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		close(sizesCh)
		out := outcome{}
		for s := range sizesCh {
			out.sizes = append(out.sizes, s)
			out.total += s
		}
		out.mean = float64(out.total) / float64(len(out.sizes))
		return out
	}

	t.Run("overloaded", func(t *testing.T) {
		got := run(t, slowConsumer, true)
		t.Logf("eager: nmorsels=%d mean=%.2f", len(got.sizes), got.mean)
		if got.total != lines {
			t.Fatalf("total = %d, want %d", got.total, lines)
		}
		// Steady state must beat the warm-up transient (mostly singles)
		// while staying below all-full (the eager first line is single).
		if got.mean <= 3 || got.mean >= 7.9 {
			t.Fatalf("mean = %.2f, want converged batching (3, 7.9)", got.mean)
		}
		ctl := run(t, slowConsumer, false)
		t.Logf("batched: nmorsels=%d mean=%.2f", len(ctl.sizes), ctl.mean)
		if ctl.mean != morselSize {
			t.Fatalf("batched mean = %.2f, want %d", ctl.mean, morselSize)
		}
	})

	t.Run("starved", func(t *testing.T) {
		got := run(t, fastConsumer, true)
		t.Logf("eager: nmorsels=%d mean=%.2f", len(got.sizes), got.mean)
		if got.total != lines {
			t.Fatalf("total = %d, want %d", got.total, lines)
		}
		if got.mean >= 2 {
			t.Fatalf("mean = %.2f, want small morsels (< 2)", got.mean)
		}
		ctl := run(t, fastConsumer, false)
		t.Logf("batched: nmorsels=%d mean=%.2f", len(ctl.sizes), ctl.mean)
		if ctl.mean != morselSize {
			t.Fatalf("batched mean = %.2f, want %d", ctl.mean, morselSize)
		}
	})
}
