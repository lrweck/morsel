package engine

// Work is the scheduler's unit of work. It holds a zero-copy view of the
// elements: a slice source stores data[start:end] (a sub-slice, which is just a
// header), while an iterator source stores a materialized batch.
//
// Release, when non-nil, is called once after the morsel has been processed. A
// source that recycles element buffers (see ChunksPooled) uses it to return them
// to its pool, so its elements are only valid until Release runs.
type Work[T any] struct {
	Start int
	// Sequence is the morsel's logical position in the source, assigned
	// serially by the single producer (no atomic). Unlike Start it is
	// meaningful for sources without random access, so an ordered batch
	// consumer can index by it: parts[Sequence].
	Sequence uint64
	Items    []T
	Release  func()
}
