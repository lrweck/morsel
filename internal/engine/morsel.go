package engine

// Work is the scheduler's unit of work. It holds a zero-copy view of the
// elements: a slice source stores data[start:end] (a sub-slice, which is just a
// header), while an iterator source stores a materialized batch. Keeping only
// Start plus Items makes the morsel 32 bytes, so the bounded queues are half
// the size of a start/end/data/items layout.
type Work[T any] struct {
	Start int
	Items []T
}
