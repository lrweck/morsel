// Package morsel provides morsel-driven parallelism for Go.
//
// Work is split into fixed-size morsels and handed to an elastic pool of
// workers. Each worker owns a bounded queue and steals from its peers when its
// own queue runs dry; the pool grows on demand and parks workers that find no
// work. Every queue is bounded, so memory never scales with the input size. The
// design follows Leis et al., "Morsel-Driven Parallelism" (SIGMOD 2014).
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
// Collect and Reduce consume it.
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

// QueueCapacity sets the bounded capacity of each worker queue and of the
// injection queue.
func QueueCapacity(n uint) Option { return func(o *options) { o.QueueCapacity = n } }

// StealAttempts bounds how many victims a worker probes before parking.
func StealAttempts(n uint) Option { return func(o *options) { o.StealAttempts = n } }

// RecoverPanics converts worker panics into errors.
func RecoverPanics(enable bool) Option {
	return func(o *options) { o.RecoverPanics = enable }
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
