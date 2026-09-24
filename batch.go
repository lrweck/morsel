package morsel

// Batch is a materialized batch of elements. Iterators need it because
// iter.Seq is single-pass and offers no random access.
type Batch[T any] struct {
	Items []T
	// Release, when non-nil, is called once by the engine after the batch's
	// elements have been processed. A Source that recycles element buffers —
	// ChunksPooled is one — sets it to return them to its pool, so its elements
	// are only valid until Release runs. Leave it nil for an ordinary batch,
	// whose elements the caller owns.
	Release func()
}

// Source yields morsels. It is consumed by a single producer, so it need not
// be safe for concurrent use. Returning an error aborts the run.
type Source[T any] func(yield func(Batch[T]) bool) error
