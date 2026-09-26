package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// waitFor spins until cond is true, yielding the processor between checks, and
// fails the test instead of hanging if cond never becomes true. It replaces a
// sleep with a deterministic-ish guard: the conditions are all "the scheduler
// made observable progress", so spinning is enough and never imposes an
// ordering.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; !cond(); i++ {
		if i >= 1<<24 {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// backpressurePending is the largest Pending value a run with
// MaxWorkers 2 and QueueCapacity 1 can reach while the producer is blocked on a
// full pool: one morsel can be executing on each of the two workers, each
// worker queue holds roundUpPow2(1)=2, the injector holds 2, and the Publish
// call that blocks has already incremented Pending. 2 + 4 + 2 + 1 = 9.
const backpressurePending = 9

// TestStressParkPublishRace forces the exact interleaving in which a worker has
// committed to parking but has not yet entered cond.Wait, and the producer
// publishes and signals before it does. With the predicate checked under the
// lock the signal is "lost", but w.ready retains it, so the worker must skip
// the wait and still execute the morsel exactly once. Repeated thousands of
// times to catch a scheduling-dependent lost wake-up.
func TestStressParkPublishRace(t *testing.T) {
	const iterations = 2000

	arrived := make(chan struct{})
	release := make(chan struct{})
	parkHook = func() {
		arrived <- struct{}{}
		<-release
	}
	t.Cleanup(func() { parkHook = nil })

	var executed atomic.Int64
	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 1, MorselSize: 1, QueueCapacity: 1, StealAttempts: 1},
		t.Context(),
		func(*Worker[struct{}, struct{}], Work[struct{}]) error {
			executed.Add(1)
			return nil
		},
		func() struct{} { return struct{}{} },
	)
	// Start the lone worker directly so it reaches park before the first
	// publish; Publish would otherwise spawn it as a side effect.
	r.spawn()

	for i := 0; i < iterations; i++ {
		<-arrived // worker is about to block
		if !r.Publish(Work[struct{}]{}) {
			t.Fatalf("publish %d failed", i)
		}
		release <- struct{}{} // let it observe the published morsel
	}
	<-arrived // worker parked again after the last morsel
	r.Done()
	release <- struct{}{}

	stats, err := r.Wait()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if got := executed.Load(); got != iterations {
		t.Fatalf("executed = %d, want %d", got, iterations)
	}
	if stats.MorselsExecuted != iterations {
		t.Fatalf("stats.MorselsExecuted = %d, want %d", stats.MorselsExecuted, iterations)
	}
}

// TestStressBackpressure fills the worker queues and the injector until the
// producer blocks, then frees one executing worker and checks the producer
// resumes and every morsel still runs exactly once.
func TestStressBackpressure(t *testing.T) {
	const total = 64

	release := make(chan struct{})
	var executed atomic.Int64
	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 1, StealAttempts: 1},
		t.Context(),
		func(*Worker[struct{}, struct{}], Work[struct{}]) error {
			<-release
			executed.Add(1)
			return nil
		},
		func() struct{} { return struct{}{} },
	)

	var published atomic.Int64
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < total; i++ {
			if !r.Publish(Work[struct{}]{}) {
				return
			}
			published.Add(1)
		}
		r.Done()
	}()

	waitFor(t, "producer to block on a full pool", func() bool {
		return r.Pending() >= backpressurePending
	})
	blockedAt := published.Load()
	if blockedAt >= total {
		t.Fatalf("producer never blocked: published %d of %d", blockedAt, total)
	}

	// Release the blocked workers; the producer must resume once one of them
	// pops an item and frees a slot.
	close(release)
	waitFor(t, "producer to resume after a worker consumed an item", func() bool {
		return published.Load() > blockedAt
	})

	<-producerDone
	stats, err := r.Wait()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if got := executed.Load(); got != total {
		t.Fatalf("executed = %d, want %d", got, total)
	}
	if stats.MorselsExecuted != total {
		t.Fatalf("stats.MorselsExecuted = %d, want %d", stats.MorselsExecuted, total)
	}
}

// TestStressStealing parks worker 0 inside a callback with a full queue, then
// spawns idle workers that must steal its morsels. Every morsel runs exactly
// once and StealsSucceeded is non-zero.
func TestStressStealing(t *testing.T) {
	const (
		total   = 64
		workers = 4
	)

	seen := make([]atomic.Int32, total)
	w0Started := make(chan struct{})
	releaseW0 := make(chan struct{})
	var w0Once sync.Once

	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: workers, MorselSize: 1, QueueCapacity: 64, StealAttempts: 8},
		t.Context(),
		func(w *Worker[struct{}, struct{}], m Work[struct{}]) error {
			seen[m.Start].Add(1)
			if w.id == 0 {
				w0Once.Do(func() { close(w0Started) })
				<-releaseW0
			}
			return nil
		},
		func() struct{} { return struct{}{} },
	)

	r.spawn() // worker 0
	w0 := r.workers[0]
	for i := 0; i < total; i++ {
		if !w0.queue.Push(Work[struct{}]{Start: i}) {
			t.Fatalf("push %d into worker 0 queue failed", i)
		}
		r.pending.Add(1)
	}
	r.notify(w0)
	<-w0Started // worker 0 holds one morsel and is blocked in process

	// Spawn the idle workers. Their own queues are empty, so the only way to
	// make progress is to steal from worker 0.
	for i := 1; i < workers; i++ {
		r.spawn()
	}
	live := int(r.live.Load())
	// A thief that misses worker 0 by chance would otherwise park; keep
	// nudging them awake until they have drained all but worker 0's morsel.
	for r.Pending() > 1 {
		for i := 1; i < live; i++ {
			r.notify(r.workers[i])
		}
		runtime.Gosched()
	}

	close(releaseW0)
	r.Done()
	stats, err := r.Wait()
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	for i := range seen {
		if got := seen[i].Load(); got != 1 {
			t.Fatalf("morsel %d executed %d times, want 1", i, got)
		}
	}
	if stats.StealsSucceeded == 0 {
		t.Fatalf("no successful steals; stats = %+v", stats)
	}
}

// TestStressCancelWhilePublishing cancels while the producer is blocked on a
// full pool. The producer must stop, Wait must return context.Canceled, and no
// callback may still be running once Wait returns.
func TestStressCancelWhilePublishing(t *testing.T) {
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var active atomic.Int64
	var waitReturned atomic.Bool
	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 1, StealAttempts: 1},
		ctx,
		func(*Worker[struct{}, struct{}], Work[struct{}]) error {
			if waitReturned.Load() {
				t.Error("callback started after Wait returned")
			}
			active.Add(1)
			<-release
			active.Add(-1)
			return nil
		},
		func() struct{} { return struct{}{} },
	)

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for {
			if !r.Publish(Work[struct{}]{}) {
				return
			}
		}
	}()

	waitFor(t, "producer to block on a full pool", func() bool {
		return r.Pending() >= backpressurePending
	})
	cancel()
	<-producerDone
	close(release) // let the already-started callbacks finish

	_, err := r.Wait()
	waitReturned.Store(true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := active.Load(); n != 0 {
		t.Fatalf("callbacks still running after Wait: %d", n)
	}
}

// TestStressCancelWhileExecuting cancels while callbacks are on the stack and
// checks the run still terminates cleanly.
func TestStressCancelWhileExecuting(t *testing.T) {
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var active atomic.Int64
	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 8, StealAttempts: 2},
		ctx,
		func(*Worker[struct{}, struct{}], Work[struct{}]) error {
			select {
			case started <- struct{}{}:
			default:
			}
			active.Add(1)
			<-release
			active.Add(-1)
			return nil
		},
		func() struct{} { return struct{}{} },
	)

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := 0; i < 100_000; i++ {
			if !r.Publish(Work[struct{}]{}) {
				return
			}
		}
		r.Done()
	}()

	<-started // at least one callback is on the stack
	cancel()
	close(release)

	_, err := r.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if n := active.Load(); n != 0 {
		t.Fatalf("callbacks still running after Wait: %d", n)
	}
	<-producerDone
}

// TestStressCancelWhileParked cancels after the workers have finished their
// work and gone to sleep. Wait must return context.Canceled rather than block.
func TestStressCancelWhileParked(t *testing.T) {
	parked := make(chan struct{})
	var once sync.Once
	parkHook = func() { once.Do(func() { close(parked) }) }
	t.Cleanup(func() { parkHook = nil })

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 8, StealAttempts: 2},
		ctx,
		func(*Worker[struct{}, struct{}], Work[struct{}]) error { return nil },
		func() struct{} { return struct{}{} },
	)
	for i := 0; i < 4; i++ {
		if !r.Publish(Work[struct{}]{}) {
			t.Fatalf("publish %d failed", i)
		}
	}
	// No Done: the run is kept alive only by the parked workers, so only
	// cancellation can end it.
	<-parked
	cancel()

	_, err := r.Wait()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestStressFirstErrorStopsRun has one callback fail while others succeed; the
// sentinel error must win and the run must terminate.
func TestStressFirstErrorStopsRun(t *testing.T) {
	boom := errors.New("boom")
	var calls atomic.Int64
	r := NewRunner[struct{}, struct{}](
		Config{MaxWorkers: 4, MorselSize: 1, QueueCapacity: 256, StealAttempts: 2},
		t.Context(),
		func(*Worker[struct{}, struct{}], Work[struct{}]) error {
			if calls.Add(1) == 50 {
				return boom
			}
			return nil
		},
		func() struct{} { return struct{}{} },
	)
	for i := 0; i < 512; i++ {
		if !r.Publish(Work[struct{}]{}) {
			break
		}
	}
	r.Done()

	_, err := r.Wait()
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if got := calls.Load(); got < 50 {
		t.Fatalf("only %d callbacks ran before stopping, want >= 50", got)
	}
}

// reuseState is deliberately distinct from every other engine state type so
// the runner pool used here cannot be shared with another test.
type reuseState struct {
	run   int
	sum   int
	count int
}

// TestStressRunnerReuseIsolation drives thousands of short successful runs
// through the pooled Runner path with a per-run marker and checks no worker
// state leaks from one run to the next.
func TestStressRunnerReuseIsolation(t *testing.T) {
	cfg := Config{MaxWorkers: 4, MorselSize: 2, QueueCapacity: 8, StealAttempts: 2}
	const runs = 2000
	data := []int{1, 2, 3, 4, 5, 6}
	const wantSum = 21
	const wantMorsels = 3 // len(data) / MorselSize

	for run := 0; run < runs; run++ {
		runID := run
		process := func(w *Worker[int, reuseState], m Work[int]) error {
			if w.State.run != runID {
				t.Errorf("run %d: worker carried state from run %d", runID, w.State.run)
			}
			for _, v := range m.Items {
				w.State.sum += v
				w.State.count++
			}
			return nil
		}
		newState := func() reuseState { return reuseState{run: runID} }
		merge := func(dst *reuseState, src reuseState) {
			dst.sum += src.sum
			dst.count += src.count
		}

		got, stats, err := RunSlice(cfg, t.Context(), data, process, newState, merge)
		if err != nil {
			t.Fatalf("run %d: err = %v", runID, err)
		}
		if got.run != runID {
			t.Fatalf("run %d: merged run = %d", runID, got.run)
		}
		if got.sum != wantSum {
			t.Fatalf("run %d: sum = %d, want %d (state leaked?)", runID, got.sum, wantSum)
		}
		if got.count != len(data) {
			t.Fatalf("run %d: count = %d, want %d", runID, got.count, len(data))
		}
		if stats.MorselsExecuted != wantMorsels {
			t.Fatalf("run %d: morsels = %d, want %d", runID, stats.MorselsExecuted, wantMorsels)
		}
	}
}
