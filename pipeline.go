package morsel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"iter"
	"sync"

	"github.com/lrweck/morsel/internal/engine"
)

// scratch is a per-stage pool of element buffers. A stage emits its morsel
// synchronously — the downstream consumes it before emit returns — so the
// buffer can be reused for the next morsel. After warm-up a Map/Filter/FlatMap
// stage allocates nothing per morsel.
type scratch[T any] struct {
	pool sync.Pool
}

func newScratch[T any]() *scratch[T] {
	s := new(scratch[T])
	s.pool.New = func() any { return new([]T) }
	return s
}

func (s *scratch[T]) get() *[]T  { return s.pool.Get().(*[]T) }
func (s *scratch[T]) put(b *[]T) { s.pool.Put(b) }

// Pipeline is a lazily built, typed chain of stages over one source. Src is the
// source element type and Out the current output type; Map and FlatMap move Out
// forward while Src is carried along, so a terminal can build a worker whose
// per-worker state is concrete and no value is boxed.
type Pipeline[Src, Out any] struct {
	feed  func(ex *Executor, ctx context.Context, feed engine.Publisher[Src]) error
	apply func(m engine.Work[Src], emit func(engine.Work[Out]) error) error
	// size is the known number of source items, or -1 when the source length
	// is not known ahead of time. It lets a small pipeline skip the pool.
	size int
}

// inlinePublisher adapts a publish func to an engine.Publisher for runs
// without queues. It reports no backlog, so an eager producer publishes
// every item immediately on the sequential path.
type inlinePublisher[T any] struct {
	publish func(engine.Work[T]) bool
}

func (p inlinePublisher[T]) Publish(m engine.Work[T]) bool { return p.publish(m) }
func (p inlinePublisher[T]) Pending() int64                { return 0 }

// run wires the source into a fresh engine run and drives it to completion.
//
// When parallelism cannot help — the source is a single morsel, or MaxWorkers
// is 1 — the whole pipeline runs in order on the calling goroutine, with no
// pool, queues or extra goroutines.
func (p Pipeline[Src, Out]) run[S any](
	ctx context.Context,
	ex *Executor,
	process func(w *engine.Worker[Src, S], m engine.Work[Src]) error,
	newState func() S,
	merge func(dst *S, src S),
) (S, error) {
	if int(ex.cfg.MaxWorkers) <= 1 || (p.size >= 0 && p.size <= int(ex.cfg.MorselSize)) {
		return p.runSequential(ctx, ex, process, newState, merge)
	}
	r := engine.NewRunner(ex.cfg, ctx, process, newState)
	feedErr := p.feed(ex, ctx, r)
	r.Done()
	stats, waitErr := r.Wait()
	ex.record(stats)
	if feedErr != nil {
		return r.Merge(merge), feedErr
	}
	return r.Merge(merge), waitErr
}

// runSequential feeds the source straight into process on the caller.
func (p Pipeline[Src, Out]) runSequential[S any](
	ctx context.Context,
	ex *Executor,
	process func(w *engine.Worker[Src, S], m engine.Work[Src]) error,
	newState func() S,
	merge func(dst *S, src S),
) (S, error) {
	w := &engine.Worker[Src, S]{State: newState()}
	var runErr error
	var morsels uint64
	feedErr := p.feed(ex, ctx, inlinePublisher[Src]{publish: func(m engine.Work[Src]) bool {
		if ctx.Err() != nil {
			runErr = ctx.Err()
			return false
		}
		morsels++
		err := process(w, m)
		if m.Release != nil {
			m.Release()
		}
		if err != nil {
			runErr = err
			return false
		}
		return true
	}})
	acc := newState()
	merge(&acc, w.State)
	ex.record(engine.Stats{MorselsCreated: morsels, MorselsExecuted: morsels})
	if feedErr != nil {
		return acc, feedErr
	}
	return acc, runErr
}

func identity[T any](m engine.Work[T], emit func(engine.Work[T]) error) error { return emit(m) }

// Slice produces the elements of data, sub-slicing by index without copying.
func Slice[T any](data []T) Pipeline[T, T] {
	return Pipeline[T, T]{
		size: len(data),
		feed: func(ex *Executor, ctx context.Context, feed engine.Publisher[T]) error {
			size := int(ex.cfg.MorselSize)
			var sequence uint64
			for start := 0; start < len(data); start += size {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				end := min(start+size, len(data))
				m := engine.Work[T]{Start: start, Sequence: sequence, Items: data[start:end]}
				sequence++
				if !feed.Publish(m) {
					return nil
				}
			}
			return nil
		},
		apply: identity[T],
	}
}

// rangeSize returns the number of integers in [start, end) when that count is
// representable as an int. The subtraction is done in uint64, so wide ranges
// such as [math.MinInt, math.MaxInt) cannot overflow a signed int; those report
// ok false and the caller treats the length as unknown. end < start is an empty
// range and also reports false, keeping the size-based fast path off.
func rangeSize(start, end int) (int, bool) {
	if end < start {
		return 0, false
	}
	n := uint64(end) - uint64(start)
	if n > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(n), true
}

// nextRangeEnd returns the exclusive end of the morsel starting at lo: the
// smaller of lo+size and end. The arithmetic is done in uint64, so it cannot
// overflow a signed int when lo or end is near math.MaxInt.
func nextRangeEnd(lo, end int, size uint) int {
	remaining := uint64(end) - uint64(lo)
	if uint64(size) >= remaining {
		return end
	}
	return int(uint64(lo) + uint64(size))
}

// Range produces the integers in [start, end).
func Range(start, end int) Pipeline[int, int] {
	size, known := rangeSize(start, end)
	if !known {
		size = -1
	}
	return Pipeline[int, int]{
		size: size,
		feed: func(ex *Executor, ctx context.Context, feed engine.Publisher[int]) error {
			step := ex.cfg.MorselSize
			var sequence uint64
			for lo := start; lo < end; {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				hi := nextRangeEnd(lo, end, step)
				items := make([]int, hi-lo)
				for i := range items {
					items[i] = lo + i
				}
				m := engine.Work[int]{Sequence: sequence, Items: items}
				sequence++
				if !feed.Publish(m) {
					return nil
				}
				lo = hi
			}
			return nil
		},
		apply: identity[int],
	}
}

// Iter produces the elements of an iterator, batched by MorselSize.
func Iter[T any](seq iter.Seq[T]) Pipeline[T, T] {
	return Pipeline[T, T]{
		size: -1,
		feed: func(ex *Executor, ctx context.Context, feed engine.Publisher[T]) error {
			if seq == nil {
				return ErrNilSource
			}
			return feedSeq(ex, ctx, feed, func(yield func(T, error) bool) {
				seq(func(v T) bool { return yield(v, nil) })
			})
		},
		apply: identity[T],
	}
}

// IterErr is Iter for a fallible iterator.
func IterErr[T any](seq iter.Seq2[T, error]) Pipeline[T, T] {
	return Pipeline[T, T]{
		size: -1,
		feed: func(ex *Executor, ctx context.Context, feed engine.Publisher[T]) error {
			if seq == nil {
				return ErrNilSource
			}
			return feedSeq(ex, ctx, feed, seq)
		},
		apply: identity[T],
	}
}

// From wraps a custom morsel source.
func From[T any](src Source[T]) Pipeline[T, T] {
	return Pipeline[T, T]{
		size: -1,
		feed: func(_ *Executor, ctx context.Context, feed engine.Publisher[T]) error {
			if src == nil {
				return ErrNilSource
			}
			var sequence uint64
			return src(func(m Batch[T]) bool {
				if ctx.Err() != nil {
					releaseBatch(m)
					return false
				}
				if !feed.Publish(engine.Work[T]{Sequence: sequence, Items: m.Items, Release: m.Release}) {
					// Not taken: the engine will never see it, so release here.
					releaseBatch(m)
					return false
				}
				sequence++
				return true
			})
		},
		apply: identity[T],
	}
}

// releaseBatch runs a batch's release hook, if it has one.
func releaseBatch[T any](m Batch[T]) {
	if m.Release != nil {
		m.Release()
	}
}

// Chunks reads r and emits one []byte per Read, so a morsel carries a single
// chunk. size defaults to 64 KiB.
//
// Every chunk is a fresh, owned slice, so the reader allocates O(reader size)
// in total and the caller may keep the bytes. Use ChunksPooled when the callback
// consumes the chunk and total allocation matters.
func Chunks(r io.Reader, size int) Pipeline[[]byte, []byte] {
	if size <= 0 {
		size = 64 << 10
	}
	return From(func(yield func(Batch[[]byte]) bool) error {
		if r == nil {
			return ErrNilSource
		}
		buf := make([]byte, size)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				if !yield(Batch[[]byte]{Items: [][]byte{chunk}}) {
					return nil
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	})
}

// chunkBuf is one recycled chunk buffer. It carries its own release func and a
// one-element Items slice, both built once when the buffer is created, so
// handing a chunk to the engine and taking it back allocates nothing per chunk.
type chunkBuf struct {
	buf     []byte
	items   [1][]byte
	release func()
}

// ChunksPooled is Chunks for a callback that consumes the chunk rather than
// keeping it. It recycles buffers, so total allocation is bounded by the chunks
// in flight instead of by the size of the reader:
//
//	ChunksPooled(file, 64<<10).ForEach(func(b []byte) { hash.Write(b) })
//
// The chunk is only valid until the callback returns; copy it to keep it. Pair
// it with a smaller QueueCapacity if the in-flight window still holds too much.
func ChunksPooled(r io.Reader, size int) Pipeline[[]byte, []byte] {
	if size <= 0 {
		size = 64 << 10
	}
	return Pipeline[[]byte, []byte]{
		size: -1,
		feed: func(_ *Executor, ctx context.Context, feed engine.Publisher[[]byte]) error {
			if r == nil {
				return ErrNilSource
			}
			// One buffer per chunk in flight: the engine releases each one as
			// soon as its morsel is done, and a released buffer is reused for a
			// later Read.
			pool := &sync.Pool{}
			pool.New = func() any {
				cb := &chunkBuf{buf: make([]byte, size)}
				cb.release = func() { pool.Put(cb) }
				return cb
			}
			var sequence uint64
			for {
				cb := pool.Get().(*chunkBuf)
				n, err := r.Read(cb.buf)
				if n > 0 {
					cb.items[0] = cb.buf[:n]
					m := engine.Work[[]byte]{Sequence: sequence, Items: cb.items[:], Release: cb.release}
					if !feed.Publish(m) {
						pool.Put(cb)
						return nil
					}
					sequence++
				} else {
					pool.Put(cb)
				}
				if err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
			}
		},
		apply: identity[[]byte],
	}
}

// Lines reads r line by line, batched by MorselSize.
func Lines(r io.Reader) Pipeline[string, string] {
	return IterErr(func(yield func(string, error) bool) {
		if r == nil {
			yield("", ErrNilSource)
			return
		}
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			if !yield(scanner.Text(), nil) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			yield("", err)
		}
	})
}

// Rows wraps a pull-style row iterator, for example over database/sql or
// encoding/csv. next returns the next row, false when exhausted, or an error.
func Rows[T any](next func() (T, bool, error)) Pipeline[T, T] {
	return IterErr(func(yield func(T, error) bool) {
		if next == nil {
			var zero T
			yield(zero, ErrNilSource)
			return
		}
		for {
			v, ok, err := next()
			if err != nil {
				yield(v, err)
				return
			}
			if !ok {
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	})
}

// feedSeq materializes a fallible iterator into morsels of MorselSize items.
func feedSeq[T any](
	ex *Executor,
	ctx context.Context,
	feed engine.Publisher[T],
	seq iter.Seq2[T, error],
) error {
	size := int(ex.cfg.MorselSize)
	buf := make([]T, 0, size)
	aborted := false
	eager := ex.cfg.Eager
	var seqErr error
	var sequence uint64
	seq(func(v T, err error) bool {
		if err != nil {
			seqErr = err
			return false
		}
		if ctx.Err() != nil {
			aborted = true
			return false
		}
		buf = append(buf, v)
		// Eager: publish a partial morsel when no work is outstanding —
		// workers would otherwise idle. Under load the buffer fills to
		// MorselSize as usual.
		if len(buf) < size && !(eager && feed.Pending() == 0) {
			return true
		}
		m := engine.Work[T]{Sequence: sequence, Items: buf}
		if !feed.Publish(m) {
			aborted = true
			return false
		}
		sequence++
		if len(m.Items) < size {
			// Partial (eager) morsel: stay small, regrow on demand.
			buf = nil
		} else {
			buf = make([]T, 0, size)
		}
		return true
	})
	if !aborted && len(buf) > 0 {
		feed.Publish(engine.Work[T]{Sequence: sequence, Items: buf})
	}
	return seqErr
}

// Map transforms each element with fn.
func (p Pipeline[Src, T]) Map[U any](fn func(T) U) Pipeline[Src, U] {
	scratch := newScratch[U]()
	return Pipeline[Src, U]{
		size: p.size,
		feed: p.feed,
		apply: func(m engine.Work[Src], emit func(engine.Work[U]) error) error {
			if fn == nil {
				return ErrNilFunction
			}
			return p.apply(m, func(out engine.Work[T]) error {
				b := scratch.get()
				buf := (*b)[:0]
				for _, v := range out.Items {
					buf = append(buf, fn(v))
				}
				err := emit(engine.Work[U]{Items: buf})
				*b = buf
				scratch.put(b)
				return err
			})
		},
	}
}

// MapE transforms each element with a fallible fn.
func (p Pipeline[Src, T]) MapE[U any](fn func(T) (U, error)) Pipeline[Src, U] {
	scratch := newScratch[U]()
	return Pipeline[Src, U]{
		size: p.size,
		feed: p.feed,
		apply: func(m engine.Work[Src], emit func(engine.Work[U]) error) error {
			if fn == nil {
				return ErrNilFunction
			}
			return p.apply(m, func(out engine.Work[T]) error {
				b := scratch.get()
				buf := (*b)[:0]
				var err error
				for _, v := range out.Items {
					if err != nil {
						break
					}
					var u U
					u, err = fn(v)
					if err == nil {
						buf = append(buf, u)
					}
				}
				if err != nil || len(buf) == 0 {
					*b = buf
					scratch.put(b)
					return err
				}
				err = emit(engine.Work[U]{Items: buf})
				*b = buf
				scratch.put(b)
				return err
			})
		},
	}
}

// Filter keeps the elements for which pred is true.
func (p Pipeline[Src, T]) Filter(pred func(T) bool) Pipeline[Src, T] {
	scratch := newScratch[T]()
	return Pipeline[Src, T]{
		size: p.size,
		feed: p.feed,
		apply: func(m engine.Work[Src], emit func(engine.Work[T]) error) error {
			if pred == nil {
				return ErrNilFunction
			}
			return p.apply(m, func(out engine.Work[T]) error {
				b := scratch.get()
				buf := (*b)[:0]
				for _, v := range out.Items {
					if pred(v) {
						buf = append(buf, v)
					}
				}
				if len(buf) == 0 {
					*b = buf
					scratch.put(b)
					return nil
				}
				err := emit(engine.Work[T]{Items: buf})
				*b = buf
				scratch.put(b)
				return err
			})
		},
	}
}

// FlatMap transforms each element into zero or more elements.
func (p Pipeline[Src, T]) FlatMap[U any](fn func(T) []U) Pipeline[Src, U] {
	scratch := newScratch[U]()
	return Pipeline[Src, U]{
		size: p.size,
		feed: p.feed,
		apply: func(m engine.Work[Src], emit func(engine.Work[U]) error) error {
			if fn == nil {
				return ErrNilFunction
			}
			return p.apply(m, func(out engine.Work[T]) error {
				b := scratch.get()
				buf := (*b)[:0]
				for _, v := range out.Items {
					buf = append(buf, fn(v)...)
				}
				if len(buf) == 0 {
					*b = buf
					scratch.put(b)
					return nil
				}
				err := emit(engine.Work[U]{Items: buf})
				*b = buf
				scratch.put(b)
				return err
			})
		},
	}
}

// batchStage wires a per-morsel transform into the pipeline. fn receives the
// morsel's items and its return value is emitted as the next morsel, so it
// may return any number of outputs (including none, which drops the morsel).
// The batch is only valid during the call; do not retain it.
func batchStage[Src, T, U any](p Pipeline[Src, T], fn func([]T) []U) Pipeline[Src, U] {
	return Pipeline[Src, U]{
		size: p.size,
		feed: p.feed,
		apply: func(m engine.Work[Src], emit func(engine.Work[U]) error) error {
			if fn == nil {
				return ErrNilFunction
			}
			return p.apply(m, func(out engine.Work[T]) error {
				res := fn(out.Items)
				if len(res) == 0 {
					return nil
				}
				return emit(engine.Work[U]{Items: res})
			})
		},
	}
}

// MapBatch transforms each morsel with fn. Unlike Map it may return any
// number of outputs per batch.
func (p Pipeline[Src, T]) MapBatch[U any](fn func([]T) []U) Pipeline[Src, U] {
	return batchStage(p, fn)
}

// FilterBatch keeps the elements fn returns, dropping the rest of the morsel.
func (p Pipeline[Src, T]) FilterBatch(fn func([]T) []T) Pipeline[Src, T] {
	return batchStage(p, fn)
}

// FlatMapBatch transforms each morsel into zero or more elements.
func (p Pipeline[Src, T]) FlatMapBatch[U any](fn func([]T) []U) Pipeline[Src, U] {
	return batchStage(p, fn)
}

// ForEach consumes the pipeline, applying fn to every element.
func (p Pipeline[Src, Out]) ForEach(fn func(Out), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	return p.ForEachE(func(v Out) error { fn(v); return nil }, opts...)
}

// ForEachE is ForEach for a fallible fn. The first error aborts the run.
func (p Pipeline[Src, Out]) ForEachE(fn func(Out) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	process := func(_ *engine.Worker[Src, struct{}], m engine.Work[Src]) error {
		return p.apply(m, func(out engine.Work[Out]) error {
			for _, v := range out.Items {
				if err := fn(v); err != nil {
					return err
				}
			}
			return nil
		})
	}
	_, err := p.run(ctx, ex, process, emptyState[Src], emptyMerge[Src])
	return err
}

// ForEachBatch consumes the pipeline, applying fn once per morsel with the
// morsel's items. The batch is only valid during the call; do not retain it.
func (p Pipeline[Src, Out]) ForEachBatch(fn func([]Out), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	return p.ForEachEBatch(func(b []Out) error { fn(b); return nil }, opts...)
}

// ForEachEBatch is ForEachBatch for a fallible fn. The first error aborts
// the run.
func (p Pipeline[Src, Out]) ForEachEBatch(fn func([]Out) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	process := func(_ *engine.Worker[Src, struct{}], m engine.Work[Src]) error {
		return p.apply(m, func(out engine.Work[Out]) error {
			return fn(out.Items)
		})
	}
	_, err := p.run(ctx, ex, process, emptyState[Src], emptyMerge[Src])
	return err
}

// Collect gathers every output element. Order is not defined; it is the
// concatenation of per-worker partials.
func (p Pipeline[Src, Out]) Collect(opts ...Option) ([]Out, error) {
	ctx, ex := resolve(opts)
	process := func(w *engine.Worker[Src, []Out], m engine.Work[Src]) error {
		return p.apply(m, func(out engine.Work[Out]) error {
			w.State = append(w.State, out.Items...)
			return nil
		})
	}
	return p.run(ctx, ex, process,
		func() []Out { return nil },
		func(dst *[]Out, src []Out) { *dst = append(*dst, src...) })
}

// Reduce folds every output element. Each worker folds its own morsels into a
// private accumulator with fold, and the accumulators are combined with merge,
// so no accumulator is shared. init must be an identity for fold and merge,
// and both should be associative and commutative because morsel order is not
// defined.
func (p Pipeline[Src, Out]) Reduce[U any](
	init U,
	fold func(U, Out) U,
	merge func(U, U) U,
	opts ...Option,
) (U, error) {
	if fold == nil || merge == nil {
		return init, ErrNilFunction
	}
	ctx, ex := resolve(opts)
	process := func(w *engine.Worker[Src, U], m engine.Work[Src]) error {
		return p.apply(m, func(out engine.Work[Out]) error {
			for _, v := range out.Items {
				w.State = fold(w.State, v)
			}
			return nil
		})
	}
	return p.run(ctx, ex, process,
		func() U { return init },
		func(dst *U, src U) { *dst = merge(*dst, src) })
}

// ReduceBatch is Reduce with fold applied once per morsel over the whole
// batch. The batch is only valid during the call; do not retain it.
func (p Pipeline[Src, Out]) ReduceBatch[U any](
	init U,
	fold func(U, []Out) U,
	merge func(U, U) U,
	opts ...Option,
) (U, error) {
	if fold == nil || merge == nil {
		return init, ErrNilFunction
	}
	ctx, ex := resolve(opts)
	process := func(w *engine.Worker[Src, U], m engine.Work[Src]) error {
		return p.apply(m, func(out engine.Work[Out]) error {
			w.State = fold(w.State, out.Items)
			return nil
		})
	}
	return p.run(ctx, ex, process,
		func() U { return init },
		func(dst *U, src U) { *dst = merge(*dst, src) })
}
