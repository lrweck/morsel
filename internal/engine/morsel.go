package engine

// WorkKind says how a morsel carries its elements. It is internal scheduler
// state; the public API never exposes it.
type WorkKind uint8

const (
	// WorkItems is a morsel whose elements are Items.
	WorkItems WorkKind = iota
	// WorkIntRange is a morsel whose elements are Start, Start+1, ..., End-1.
	// It is only ever produced for an int range source, so T is int. The
	// engine materializes it before the process callback runs.
	WorkIntRange
)

// Work is the scheduler's unit of work. It holds a zero-copy view of the
// elements: a slice source stores data[start:end] (a sub-slice, which is just a
// header), while an iterator source stores a materialized batch.
//
// Release, when non-nil, is called once after the morsel has been processed. A
// source that recycles element buffers (see ChunksPooled) uses it to return them
// to its pool, so its elements are only valid until Release runs.
type Work[T any] struct {
	Kind  WorkKind
	Start int
	End   int
	// Sequence is the morsel's logical position in the source, assigned
	// serially by the single producer (no atomic). Unlike Start it is
	// meaningful for sources without random access, so an ordered batch
	// consumer can index by it: parts[Sequence].
	Sequence uint64
	Items    []T
	// Release, when non-nil, is called once after the morsel has been
	// processed.
	Release func()
}

// MaterializeRange replaces an int-range morsel with an items morsel built in
// buf, returning buf for reuse. buf is growable; the caller must not use it
// after the morsel is processed. It is a no-op for an ordinary items morsel.
//
// It is exported only within the internal engine: the pipeline's sequential
// path needs the same materialization the worker path does. Because the
// elements of a range are ints, the type assertions below only succeed when T
// is int, which is the only way WorkIntRange is ever created.
func MaterializeRange[T any](m Work[T], buf []int) (Work[T], []int) {
	if m.Kind != WorkIntRange {
		return m, buf
	}
	r, ok := any(m).(Work[int])
	assert(ok, "int range morsel must have int elements")
	n := r.End - r.Start
	if cap(buf) < n {
		buf = make([]int, 0, n)
	}
	buf = buf[:0]
	for i := r.Start; i < r.End; i++ {
		buf = append(buf, i)
	}
	r.Items = buf
	out, ok := any(r).(Work[T])
	assert(ok, "int range morsel must have int elements")
	return out, buf
}
