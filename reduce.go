package morsel

import (
	"context"
	"iter"

	"github.com/lrweck/morsel/internal/engine"
)

// ReduceSlice folds data into a single value. Each worker folds its own morsels
// into a private accumulator with fold, and the accumulators are combined with
// merge at the end, so no accumulator is shared between workers.
//
// init must be an identity for both fold and merge (0 for sums, 1 for
// products, nil for concatenation), because every worker starts from init and
// the merged result is folded once more from init.
func (ex *Executor) ReduceSlice[T, R any](
	ctx context.Context,
	data []T,
	init R,
	fold func(R, T) R,
	merge func(R, R) R,
) (R, error) {
	if fold == nil || merge == nil {
		return init, ErrNilFunction
	}
	process := func(w *engine.Worker[T, R], m engine.Work[T]) error {
		for _, v := range m.Items {
			w.State = fold(w.State, v)
		}
		return nil
	}
	res, stats, err := engine.RunSlice(ex.cfg, ctx, data, process,
		func() R { return init },
		func(dst *R, src R) { *dst = merge(*dst, src) })
	ex.record(stats)
	return res, err
}

// ReduceSeq is ReduceSlice for an iterator. Order is not defined across
// morsels, so fold and merge should be associative and commutative for a stable
// result.
func (ex *Executor) ReduceSeq[T, R any](
	ctx context.Context,
	seq iter.Seq[T],
	init R,
	fold func(R, T) R,
	merge func(R, R) R,
) (R, error) {
	if fold == nil || merge == nil {
		return init, ErrNilFunction
	}
	if seq == nil {
		return init, ErrNilSource
	}
	process := func(w *engine.Worker[T, R], m engine.Work[T]) error {
		for _, v := range m.Items {
			w.State = fold(w.State, v)
		}
		return nil
	}
	res, stats, err := engine.RunIter(ex.cfg, ctx, seq, process,
		func() R { return init },
		func(dst *R, src R) { *dst = merge(*dst, src) })
	ex.record(stats)
	return res, err
}

// ReduceSlice is the package-level form of Executor.ReduceSlice.
func ReduceSlice[T, R any](
	ctx context.Context,
	ex *Executor,
	data []T,
	init R,
	fold func(R, T) R,
	merge func(R, R) R,
) (R, error) {
	return ex.ReduceSlice(ctx, data, init, fold, merge)
}

// ReduceSeq is the package-level form of Executor.ReduceSeq.
func ReduceSeq[T, R any](
	ctx context.Context,
	ex *Executor,
	seq iter.Seq[T],
	init R,
	fold func(R, T) R,
	merge func(R, R) R,
) (R, error) {
	return ex.ReduceSeq(ctx, seq, init, fold, merge)
}
