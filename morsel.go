// Package morsel provides morsel-driven parallelism for Go.
//
// Work is split into fixed-size morsels and handed to an elastic pool of
// workers. Each worker owns a bounded queue and steals from its peers when its
// own queue runs dry; the pool grows on demand and parks workers that find no
// work. Every queue is bounded, so memory never scales with the input size. The
// design follows Leis et al., "Morsel-Driven Parallelism" (SIGMOD 2014); the
// README's References section lists the other works it draws on.
//
// # Two surfaces
//
// The Executor primitives give explicit control over the pool:
//
//	ex := morsel.NewExecutor(morsel.Config{MaxWorkers: 8, MorselSize: 256})
//	err := ex.ForEachSlice(ctx, debts, func(d *Debt) error {
//		return calculate(d)
//	})
//
// The fluent pipeline is typed end to end and needs no executor:
//
//	total, err := morsel.Slice(debts).
//		Map(validate).
//		Map(calculate).
//		Reduce(0, add, add)
//
// The standalone helpers take options last and are optional:
//
//	err := morsel.ForEach(debts, func(d *Debt) {
//		calculate(d)
//	}, morsel.MorselSize(256))
//
// # Sources and stages
//
// Slice, Range, Iter, IterErr, Chunks, Lines, Rows and From produce a
// [Pipeline]; Map, MapE, Filter and FlatMap transform it; ForEach, ForEachE,
// Collect and Reduce consume it. Every stage and terminal has an optional
// Batch variant (MapBatch, ForEachBatch, ...) that runs once per morsel over
// []Out instead of once per element.
//
// # Errors and cancellation
//
// A callback may return an error: the first one aborts the run and is returned,
// and work not yet started is dropped. Cancelling the context passed with
// [WithContext] does the same. Callback panics propagate by default;
// [RecoverPanics] turns them into errors.
package morsel

import (
	"context"
	"errors"
	"iter"
	"time"
)

var (
	// ErrNilFunction is returned when no callback was supplied.
	ErrNilFunction = errors.New("morsel: nil function")
	// ErrNilSource is returned when a nil source or iterator was supplied.
	ErrNilSource = errors.New("morsel: nil source")
)

// options is the resolved configuration for a run.
type options struct {
	Config
	ctx context.Context
	ex  *Executor
}

func defaultOptions() options {
	return options{Config: DefaultConfig(), ctx: context.Background()}
}

// Option configures a run.
type Option func(*options)

// MaxWorkers sets the worker ceiling.
func MaxWorkers(n uint) Option { return func(o *options) { o.MaxWorkers = n } }

// MorselSize sets the number of items per morsel.
func MorselSize(n uint) Option { return func(o *options) { o.MorselSize = n } }

// AdaptiveMorselSize enables adaptive morsel sizing: the producer resizes
// future morsels from the per-morsel processing time workers observe, aiming
// for TargetMorselTime and staying within MinMorselSize and MaxMorselSize.
func AdaptiveMorselSize(enable bool) Option {
	return func(o *options) { o.AdaptiveMorselSize = enable }
}

// MinMorselSize sets the lower bound for adaptive morsel sizing.
func MinMorselSize(n uint) Option { return func(o *options) { o.MinMorselSize = n } }

// MaxMorselSize sets the upper bound for adaptive morsel sizing.
func MaxMorselSize(n uint) Option { return func(o *options) { o.MaxMorselSize = n } }

// TargetMorselTime sets the per-morsel processing time adaptive sizing aims
// for.
func TargetMorselTime(d time.Duration) Option {
	return func(o *options) { o.TargetMorselTime = d }
}

// QueueCapacity sets the bounded capacity of each worker queue and of the
// injection queue.
func QueueCapacity(n uint) Option { return func(o *options) { o.QueueCapacity = n } }

// StealAttempts bounds how many victims a worker probes before parking.
func StealAttempts(n uint) Option { return func(o *options) { o.StealAttempts = n } }

// RecoverPanics converts worker panics into errors.
func RecoverPanics(enable bool) Option {
	return func(o *options) { o.RecoverPanics = enable }
}

// Eager publishes a partial morsel from an iterator source whenever no work
// is outstanding — workers would otherwise idle — instead of waiting to fill
// it to MorselSize. Under load morsels still fill as usual.
func Eager(enable bool) Option {
	return func(o *options) { o.Eager = enable }
}

// WithContext ties the run to ctx. Cancelling ctx aborts the run.
func WithContext(ctx context.Context) Option {
	return func(o *options) {
		if ctx != nil {
			o.ctx = ctx
		}
	}
}

// WithExecutor runs on an existing executor, ignoring the configuration
// options. Use it to reuse a pool's configuration and stats across runs.
func WithExecutor(ex *Executor) Option {
	return func(o *options) { o.ex = ex }
}

// resolve applies the options and returns the context and executor to use.
func resolve(opts []Option) (context.Context, *Executor) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	ex := o.ex
	if ex == nil {
		ex = NewExecutor(o.Config)
	}
	return o.ctx, ex
}

// ForEach applies fn to every element of data in parallel.
//
//	ForEach(data, func(v T) { ... }, MorselSize(256))
//
// For a fallible callback use ForEachE. Options come last, as is idiomatic in
// Go, and are optional.
func ForEach[T any](data []T, fn func(T), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSlice(ctx, data, func(v T) error { fn(v); return nil })
}

// ForEachE is ForEach for a fallible fn. The first error aborts the run and is
// returned.
func ForEachE[T any](data []T, fn func(T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSlice(ctx, data, fn)
}

// ForEachSeq is ForEach for an iterator.
func ForEachSeq[T any](seq iter.Seq[T], fn func(T), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeq(ctx, seq, func(v T) error { fn(v); return nil })
}

// ForEachSeqE is ForEachSeq for a fallible fn.
func ForEachSeqE[T any](seq iter.Seq[T], fn func(T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeq(ctx, seq, fn)
}

// ForEachSeqErr is ForEach for a fallible iterator.
func ForEachSeqErr[T any](seq iter.Seq2[T, error], fn func(T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeqErr(ctx, seq, fn)
}

// ForEachBatch applies fn once per morsel with the morsel's items, instead
// of once per element. The batch is only valid during the call; do not
// retain it. Morsel size is controlled by MorselSize, as usual.
func ForEachBatch[T any](data []T, fn func([]T), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSliceBatch(ctx, data, func(b []T) error { fn(b); return nil })
}

// ForEachEBatch is ForEachBatch for a fallible fn. The first error aborts
// the run and is returned.
func ForEachEBatch[T any](data []T, fn func([]T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSliceBatch(ctx, data, fn)
}

// ForEachSeqBatch is ForEachBatch for an iterator.
func ForEachSeqBatch[T any](seq iter.Seq[T], fn func([]T), opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeqBatch(ctx, seq, func(b []T) error { fn(b); return nil })
}

// ForEachSeqEBatch is ForEachSeqBatch for a fallible fn.
func ForEachSeqEBatch[T any](seq iter.Seq[T], fn func([]T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeqBatch(ctx, seq, fn)
}

// ForEachSeqErrBatch is ForEachSeqBatch for a fallible iterator.
func ForEachSeqErrBatch[T any](seq iter.Seq2[T, error], fn func([]T) error, opts ...Option) error {
	if fn == nil {
		return ErrNilFunction
	}
	ctx, ex := resolve(opts)
	return ex.ForEachSeqErrBatch(ctx, seq, fn)
}
