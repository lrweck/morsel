package engine

// Work is the scheduler's unit of work. It holds a zero-copy view of the
// elements: a slice source stores data[start:end] (a sub-slice, which is just a
// header), while an iterator source stores a materialized batch.
//
// Release, when non-nil, is called once after the morsel has been processed. A
// source that recycles element buffers (see ChunksPooled) uses it to return them
// to its pool, so its elements are only valid until Release runs.
type Work[T any] struct {
	Start   int
	Items   []T
	Release func()
}
