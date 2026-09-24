package morsel

// Batch is a materialized batch of elements. Iterators need it because
// iter.Seq is single-pass and offers no random access.
type Batch[T any] struct {
	Items []T
}

// Source yields morsels. It is consumed by a single producer, so it need not
// be safe for concurrent use. Returning an error aborts the run.
type Source[T any] func(yield func(Batch[T]) bool) error
