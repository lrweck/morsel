package engine

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/lrweck/morsel/internal/queue"
)

// Worker owns one SPMC queue. The global producer is that queue's single
// producer; the worker and any thieves are its consumers. State is the
// worker's private accumulator (Reduce/Collect), touched only by its owner.
type Worker[T, S any] struct {
	State S

	id    int
	queue *queue.SPMC[Work[T]]

	// mu/cond/ready implement the park/wake handshake. sync.Cond is markedly
	// cheaper than a channel handoff here (Signal is ~28ns vs ~120ns), and the
	// producer can wake exactly the worker it fed instead of an arbitrary one.
	mu    sync.Mutex
	cond  *sync.Cond
	ready bool

	run   *Runner[T, S]
	rng   uint64
	stats WorkerStats
}

// Runner is the logical state of one execution. It is created per run, never
// stored on the caller, so independent runs cannot interfere.
type Runner[T, S any] struct {
	ctx      context.Context
	cfg      Config
	process  func(w *Worker[T, S], m Work[T]) error
	newState func() S

	workers []*Worker[T, S]
	inject  *queue.MPMC[Work[T]] // bounded overflow queue
	space   chan struct{}        // wakes a producer blocked on a full pool
	stop    chan struct{}

	mu   sync.Mutex
	cond *sync.Cond

	next         atomic.Uint64
	live         atomic.Int32
	stopped      atomic.Bool
	producerDone atomic.Bool
	created      atomic.Uint64
	pending      atomic.Int64
	stopOnce     sync.Once
	err          error

	wg sync.WaitGroup
}

// NewRunner returns a Runner that has not started any workers yet.
func NewRunner[T, S any](
	cfg Config,
	ctx context.Context,
	process func(w *Worker[T, S], m Work[T]) error,
	newState func() S,
) *Runner[T, S] {
	cfg = cfg.Normalize()
	assert(cfg.MaxWorkers > 0, "MaxWorkers must be positive after normalize")
	assert(cfg.MorselSize > 0, "MorselSize must be positive after normalize")
	assert(cfg.QueueCapacity > 0, "QueueCapacity must be positive after normalize")
	assert(cfg.StealAttempts > 0, "StealAttempts must be positive after normalize")

	r := &Runner[T, S]{
		ctx:      ctx,
		cfg:      cfg,
		process:  process,
		newState: newState,
		inject:   queue.NewMPMC[Work[T]](int(cfg.QueueCapacity)),
		space:    make(chan struct{}, 1),
		stop:     make(chan struct{}),
		workers:  make([]*Worker[T, S], cfg.MaxWorkers),
	}
	r.cond = sync.NewCond(&r.mu)
	for i := range r.workers {
		r.workers[i] = &Worker[T, S]{
			id:  i,
			run: r,
			rng: uint64(i+1) * 0x9E3779B97F4A7C15,
		}
	}
	return r
}

func (r *Runner[T, S]) isDone() bool {
	return r.producerDone.Load() && r.pending.Load() == 0
}

// spawn lazily gives a worker its queue, wake channel and private state, then
// starts it. Allocating on spawn instead of up front keeps the per-run cost
// proportional to the workers actually used.
func (r *Runner[T, S]) spawn() {
	r.mu.Lock()
	if r.stopped.Load() || int(r.live.Load()) >= int(r.cfg.MaxWorkers) {
		r.mu.Unlock()
		return
	}
	id := int(r.live.Load())
	w := r.workers[id]
	w.queue = queue.NewSPMC[Work[T]](int(r.cfg.QueueCapacity))
	w.cond = sync.NewCond(&w.mu)
	w.State = r.newState()
	r.live.Add(1)
	r.wg.Add(1)
	r.mu.Unlock()
	go w.loop()
}

// Publish hands a morsel directly to a worker queue, round-robin, so the hot
// path is a single SPMC push instead of an injector hop. The shared MPMC
// injector is only an overflow buffer for when every queue is full. The
// producer blocks, bounded, when the pool and the overflow are both full.
func (r *Runner[T, S]) Publish(m Work[T]) bool {
	r.pending.Add(1)
	for {
		if r.stopped.Load() || r.ctx.Err() != nil {
			r.pending.Add(-1)
			return false
		}
		count := int(r.live.Load())
		if count == 0 {
			r.spawn()
			count = int(r.live.Load())
			if count == 0 {
				r.pending.Add(-1)
				return false
			}
		}
		for i := 0; i < count; i++ {
			idx := int(r.next.Add(1)-1) % count
			w := r.workers[idx]
			if w.queue.Push(m) {
				r.notify(w)
				r.created.Add(1)
				r.grow()
				return true
			}
		}
		if r.inject.Enqueue(m) {
			r.created.Add(1)
			r.grow()
			return true
		}
		if count < int(r.cfg.MaxWorkers) {
			r.spawn()
			continue
		}
		select {
		case <-r.space:
		case <-r.stop:
		case <-r.ctx.Done():
			r.pending.Add(-1)
			return false
		}
	}
}

// grow keeps the pool proportional to the outstanding work: desired workers is
// min(MaxWorkers, pending morsels), so a workload with fewer morsels than a
// queue can hold still spreads across workers instead of running on one.
func (r *Runner[T, S]) grow() {
	if int(r.live.Load()) >= int(r.cfg.MaxWorkers) {
		return
	}
	if int(r.pending.Load()) <= int(r.live.Load()) {
		return
	}
	r.spawn()
}

func (r *Runner[T, S]) notify(w *Worker[T, S]) {
	w.mu.Lock()
	w.ready = true
	w.cond.Signal()
	w.mu.Unlock()
}

// wakeAll releases every parked worker. It is called once, when the run stops.
func (r *Runner[T, S]) wakeAll() {
	r.stopOnce.Do(func() { close(r.stop) })
	for _, w := range r.workers[:int(r.live.Load())] {
		w.mu.Lock()
		w.cond.Broadcast()
		w.mu.Unlock()
	}
}

// release tells a producer blocked on a full pool that a slot has freed. It is
// a no-op whenever the signal is already pending, so it costs almost nothing on
// the common path.
func (r *Runner[T, S]) release() {
	select {
	case r.space <- struct{}{}:
	default:
	}
}

// Done marks the end of production.
func (r *Runner[T, S]) Done() {
	r.producerDone.Store(true)
	r.signal()
}

// signal wakes Wait when the run has finished.
func (r *Runner[T, S]) signal() {
	if !r.isDone() {
		return
	}
	r.mu.Lock()
	r.cond.Broadcast()
	r.mu.Unlock()
}

// fail records the first error and cancels the run: no new morsels are
// published, and workers stop after the morsel they are running.
func (r *Runner[T, S]) fail(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	first := r.err == nil
	if first {
		r.err = err
		r.stopped.Store(true)
	}
	r.cond.Broadcast()
	r.mu.Unlock()
	if first {
		r.wakeAll()
	}
}

func (w *Worker[T, S]) loop() {
	defer w.run.wg.Done()
	r := w.run
	for {
		if r.stopped.Load() || r.ctx.Err() != nil {
			return
		}
		if m, ok := w.queue.Pop(); ok {
			r.release()
			w.execute(m)
			continue
		}
		if m, ok := r.steal(w); ok {
			w.execute(m)
			continue
		}
		if m, ok := r.inject.Dequeue(); ok {
			w.execute(m)
			continue
		}
		if r.park(w) {
			return
		}
	}
}

func (w *Worker[T, S]) execute(m Work[T]) {
	r := w.run
	r.pending.Add(-1)
	w.stats.MorselsExecuted++
	if err := r.invoke(w, m); err != nil {
		r.fail(err)
	}
	r.signal()
}

// invoke runs process, optionally turning a user panic into an error. An
// assertion panic is always re-raised: a corrupt invariant must crash rather
// than be reported as an ordinary failure.
func (r *Runner[T, S]) invoke(w *Worker[T, S], m Work[T]) (err error) {
	if !r.cfg.RecoverPanics {
		return r.process(w, m)
	}
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		if failed, ok := rec.(assertionError); ok {
			panic(failed)
		}
		if e, ok := rec.(error); ok {
			err = e
			return
		}
		err = fmt.Errorf("morsel: recovered panic: %v", rec)
	}()
	return r.process(w, m)
}

// steal tries a bounded number of victims, starting from a per-worker
// pseudo-random offset so no single worker is always the target. The generator
// is a xorshift on the worker's own state: no lock, no allocation.
func (r *Runner[T, S]) steal(w *Worker[T, S]) (Work[T], bool) {
	live := int(r.live.Load())
	if live <= 1 {
		return Work[T]{}, false
	}
	attempts := min(int(r.cfg.StealAttempts), live)
	for i := 0; i < attempts; i++ {
		w.rng = xorshift(w.rng)
		victim := int(w.rng % uint64(live))
		if victim == w.id {
			continue
		}
		target := r.workers[victim].queue
		if target == nil {
			continue
		}
		if m, ok := target.Pop(); ok {
			w.stats.StealsSucceeded++
			return m, true
		}
	}
	w.stats.StealsAttempted += uint64(attempts)
	return Work[T]{}, false
}

// park blocks until the worker is notified of new local work or the run stops.
// No channel and no counter: the producer wakes exactly the worker it fed.
func (r *Runner[T, S]) park(w *Worker[T, S]) bool {
	w.mu.Lock()
	for !w.ready {
		if r.stopped.Load() {
			w.mu.Unlock()
			return true
		}
		w.cond.Wait()
	}
	w.ready = false
	w.mu.Unlock()
	return false
}

// Wait blocks until the run finishes or is aborted, then tears the pool down.
func (r *Runner[T, S]) Wait() (Stats, error) {
	if err := r.ctx.Err(); err != nil {
		r.fail(err)
	}

	// The watcher wakes Wait when an external context is cancelled, because
	// sync.Cond cannot be selected on. Its lifetime is bounded by the run.
	watchStop := make(chan struct{})
	go func() {
		select {
		case <-r.ctx.Done():
			r.fail(r.ctx.Err())
		case <-watchStop:
		}
	}()

	r.mu.Lock()
	for !r.isDone() && r.err == nil {
		r.cond.Wait()
	}
	r.mu.Unlock()

	r.stopped.Store(true)
	r.wakeAll()
	r.wg.Wait()
	close(watchStop)

	r.mu.Lock()
	err := r.err
	r.mu.Unlock()
	return r.snapshot(), err
}

// Merge folds every worker's private state into one result. It runs after Wait,
// when no worker can touch its state anymore.
func (r *Runner[T, S]) Merge(merge func(*S, S)) S {
	acc := r.newState()
	for _, w := range r.workers[:int(r.live.Load())] {
		merge(&acc, w.State)
	}
	return acc
}

func (r *Runner[T, S]) snapshot() Stats {
	live := int(r.live.Load())
	s := Stats{MorselsCreated: r.created.Load(), WorkersCreated: uint64(live)}
	for _, w := range r.workers[:live] {
		s.MorselsExecuted += w.stats.MorselsExecuted
		s.StealsAttempted += w.stats.StealsAttempted
		s.StealsSucceeded += w.stats.StealsSucceeded
	}
	return s
}

func xorshift(x uint64) uint64 {
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	return x
}

// RunSlice is the slice path: it morselizes data by index without copying, then
// runs process on each morsel.
func RunSlice[T, S any](
	cfg Config,
	ctx context.Context,
	data []T,
	process func(w *Worker[T, S], m Work[T]) error,
	newState func() S,
	merge func(dst *S, src S),
) (S, Stats, error) {
	r := NewRunner(cfg, ctx, process, newState)
	size := int(cfg.MorselSize)
	for start := 0; start < len(data); start += size {
		end := min(start+size, len(data))
		if !r.Publish(Work[T]{Start: start, Items: data[start:end]}) {
			break
		}
	}
	r.Done()
	stats, err := r.Wait()
	return r.Merge(merge), stats, err
}

// RunIter is the iterator path: a bounded producer materializes morsels of at
// most MorselSize items, so memory stays O(in-flight morsels) instead of
// O(total elements).
func RunIter[T, S any](
	cfg Config,
	ctx context.Context,
	seq iter.Seq[T],
	process func(w *Worker[T, S], m Work[T]) error,
	newState func() S,
	merge func(dst *S, src S),
) (S, Stats, error) {
	r := NewRunner(cfg, ctx, process, newState)
	size := int(cfg.MorselSize)
	buf := make([]T, 0, size)
	aborted := false
	seq(func(v T) bool {
		buf = append(buf, v)
		if len(buf) < size {
			return true
		}
		m := Work[T]{Items: buf}
		if !r.Publish(m) {
			aborted = true
			return false
		}
		buf = make([]T, 0, size)
		return true
	})
	if !aborted && len(buf) > 0 {
		r.Publish(Work[T]{Items: buf})
	}
	r.Done()
	stats, err := r.Wait()
	return r.Merge(merge), stats, err
}
