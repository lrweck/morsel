package morsel

import (
	"context"
	"iter"

	"github.com/lrweck/morsel/internal/engine"
)

// ForEachSlice applies fn to every element of data in parallel.
func (ex *Executor) ForEachSlice[T any](ctx context.Context, data []T, fn func(T) error) error {
	if fn == nil {
		return ErrNilFunction
	}
	process := func(_ *engine.Worker[T, struct{}], m engine.Work[T]) error {
		for _, v := range m.Items {
			if err := fn(v); err != nil {
				return err
			}
		}
		return nil
	}
	_, stats, err := engine.RunSlice(ex.cfg, ctx, data, process, emptyState[T], emptyMerge[T])
	ex.record(stats)
	return err
}

// ForEachSeq applies fn to every element of an iterator in parallel.
func (ex *Executor) ForEachSeq[T any](
	ctx context.Context,
	seq iter.Seq[T],
	fn func(T) error,
) error {
	if fn == nil {
		return ErrNilFunction
	}
	if seq == nil {
		return ErrNilSource
	}
	process := func(_ *engine.Worker[T, struct{}], m engine.Work[T]) error {
		for _, v := range m.Items {
			if err := fn(v); err != nil {
				return err
			}
		}
		return nil
	}
	_, stats, err := engine.RunIter(ex.cfg, ctx, seq, process, emptyState[T], emptyMerge[T])
	ex.record(stats)
	return err
}

// ForEachSeqErr is ForEachSeq for a fallible iterator.
func (ex *Executor) ForEachSeqErr[T any](
	ctx context.Context,
	seq iter.Seq2[T, error],
	fn func(T) error,
) error {
	if seq == nil {
		return ErrNilSource
	}
	var seqErr error
	runErr := ex.ForEachSeq(ctx, func(yield func(T) bool) {
		seq(func(v T, err error) bool {
			if err != nil {
				seqErr = err
				return false
			}
			return yield(v)
		})
	}, fn)
	if runErr != nil {
		return runErr
	}
	return seqErr
}

// ForEachSlice is the package-level form of Executor.ForEachSlice.
func ForEachSlice[T any](ctx context.Context, ex *Executor, data []T, fn func(T) error) error {
	return ex.ForEachSlice(ctx, data, fn)
}
