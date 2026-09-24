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
	feed  func(ex *Executor, ctx context.Context, feed func(engine.Work[Src]) bool) error
	apply func(m engine.Work[Src], emit func(engine.Work[Out]) error) error
	// size is the known number of source items, or -1 when the source length
	// is not known ahead of time. It lets a small pipeline skip the pool.
	size int
}

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
	feedErr := p.feed(ex, ctx, r.Publish)
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
	feedErr := p.feed(ex, ctx, func(m engine.Work[Src]) bool {
		if ctx.Err() != nil {
			runErr = ctx.Err()
			return false
		}
		morsels++
		if err := process(w, m); err != nil {
			runErr = err
			return false
		}
		return true
	})
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
		feed: func(ex *Executor, ctx context.Context, feed func(engine.Work[T]) bool) error {
			size := int(ex.cfg.MorselSize)
			for start := 0; start < len(data); start += size {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				end := min(start+size, len(data))
				if !feed(engine.Work[T]{Start: start, Items: data[start:end]}) {
					return nil
				}
			}
			return nil
		},
		apply: identity[T],
	}
}

// Range produces the integers in [start, end).
func Range(start, end int) Pipeline[int, int] {
	return Pipeline[int, int]{
		size: end - start,
		feed: func(ex *Executor, ctx context.Context, feed func(engine.Work[int]) bool) error {
			size := int(ex.cfg.MorselSize)
			for lo := start; lo < end; lo += size {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				hi := min(lo+size, end)
				items := make([]int, hi-lo)
				for i := range items {
					items[i] = lo + i
				}
				if !feed(engine.Work[int]{Items: items}) {
					return nil
				}
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
		feed: func(ex *Executor, ctx context.Context, feed func(engine.Work[T]) bool) error {
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
		feed: func(ex *Executor, ctx context.Context, feed func(engine.Work[T]) bool) error {
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
		feed: func(_ *Executor, ctx context.Context, feed func(engine.Work[T]) bool) error {
			if src == nil {
				return ErrNilSource
			}
			return src(func(m Batch[T]) bool {
				if ctx.Err() != nil {
					return false
				}
				return feed(engine.Work[T]{Items: m.Items})
			})
		},
		apply: identity[T],
	}
}

// Chunks reads r and emits one []byte per Read, so a morsel carries a single
// chunk. size defaults to 64 KiB.
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
	feed func(engine.Work[T]) bool,
	seq iter.Seq2[T, error],
) error {
	size := int(ex.cfg.MorselSize)
	buf := make([]T, 0, size)
	aborted := false
	var seqErr error
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
		if len(buf) < size {
			return true
		}
		m := engine.Work[T]{Items: buf}
		if !feed(m) {
			aborted = true
			return false
		}
		buf = make([]T, 0, size)
		return true
	})
	if !aborted && len(buf) > 0 {
		feed(engine.Work[T]{Items: buf})
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
