package morsel

import (
	"context"
	"iter"

	"github.com/lrweck/morsel/internal/engine"
)

// MapSlice applies fn to every element of data and returns the results in the
// same order. Each morsel writes only its own region of the result, so there
// is no append and no lock, and order is preserved despite the parallelism.
func (ex *Executor) MapSlice[T, R any](ctx context.Context, data []T, fn func(T) R) ([]R, error) {
	if fn == nil {
		return nil, ErrNilFunction
	}
	result := make([]R, len(data))
	process := func(_ *engine.Worker[T, struct{}], m engine.Work[T]) error {
		for j, v := range m.Items {
			result[m.Start+j] = fn(v)
		}
		return nil
	}
	_, stats, err := engine.RunSlice(ex.cfg, ctx, data, process, emptyState[T], emptyMerge[T])
	ex.record(stats)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MapSeq applies fn to every element of an iterator. Because an iterator has no
// random access, results are gathered per worker and concatenated, so their
// order is not defined; use MapSlice when order matters.
func (ex *Executor) MapSeq[T, R any](
	ctx context.Context,
	seq iter.Seq[T],
	fn func(T) R,
) ([]R, error) {
	if fn == nil {
		return nil, ErrNilFunction
	}
	if seq == nil {
		return nil, ErrNilSource
	}
	process := func(w *engine.Worker[T, []R], m engine.Work[T]) error {
		for _, v := range m.Items {
			w.State = append(w.State, fn(v))
		}
		return nil
	}
	out, stats, err := engine.RunIter(ex.cfg, ctx, seq, process,
		func() []R { return nil },
		func(dst *[]R, src []R) { *dst = append(*dst, src...) })
	ex.record(stats)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MapSlice is the package-level form of Executor.MapSlice.
func MapSlice[T, R any](ctx context.Context, ex *Executor, data []T, fn func(T) R) ([]R, error) {
	return ex.MapSlice(ctx, data, fn)
}

// MapSliceBatch applies fn once per morsel and returns the concatenated
// results in order. Unlike MapSlice it allows each batch to produce any
// number of outputs, so it also covers filtering and fan-out per batch.
// The batch is only valid during the call; do not retain it.
func (ex *Executor) MapSliceBatch[T, R any](ctx context.Context, data []T, fn func([]T) []R) ([]R, error) {
	if fn == nil {
		return nil, ErrNilFunction
	}
	size := int(ex.cfg.MorselSize)
	if size <= 0 {
		size = max(len(data), 1)
	}
	parts := make([][]R, (len(data)+size-1)/size)
	process := func(_ *engine.Worker[T, struct{}], m engine.Work[T]) error {
		// Sequence is the producer-assigned logical position, so the batch
		// writes its own slot: no lock, no append, order preserved.
		parts[m.Sequence] = fn(m.Items)
		return nil
	}
	_, stats, err := engine.RunSlice(ex.cfg, ctx, data, process, emptyState[T], emptyMerge[T])
	ex.record(stats)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, p := range parts {
		total += len(p)
	}
	out := make([]R, 0, total)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out, nil
}

// MapSeq is the package-level form of Executor.MapSeq.
func MapSeq[T, R any](
	ctx context.Context,
	ex *Executor,
	seq iter.Seq[T],
	fn func(T) R,
) ([]R, error) {
	return ex.MapSeq(ctx, seq, fn)
}

// MapSeqBatch is MapSliceBatch for an iterator: fn runs once per morsel and
// may return any number of outputs. Order is not defined, as in MapSeq.
// The batch is only valid during the call; do not retain it.
func (ex *Executor) MapSeqBatch[T, R any](
	ctx context.Context,
	seq iter.Seq[T],
	fn func([]T) []R,
) ([]R, error) {
	if fn == nil {
		return nil, ErrNilFunction
	}
	if seq == nil {
		return nil, ErrNilSource
	}
	process := func(w *engine.Worker[T, []R], m engine.Work[T]) error {
		w.State = append(w.State, fn(m.Items)...)
		return nil
	}
	out, stats, err := engine.RunIter(ex.cfg, ctx, seq, process,
		func() []R { return nil },
		func(dst *[]R, src []R) { *dst = append(*dst, src...) })
	ex.record(stats)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MapSliceBatch is the package-level form of Executor.MapSliceBatch.
func MapSliceBatch[T, R any](ctx context.Context, ex *Executor, data []T, fn func([]T) []R) ([]R, error) {
	return ex.MapSliceBatch(ctx, data, fn)
}

// MapSeqBatch is the package-level form of Executor.MapSeqBatch.
func MapSeqBatch[T, R any](
	ctx context.Context,
	ex *Executor,
	seq iter.Seq[T],
	fn func([]T) []R,
) ([]R, error) {
	return ex.MapSeqBatch(ctx, seq, fn)
}
