// Package queue provides the bounded, lock-free queues that back the parallel
// scheduler. It is internal: only the parent morsel package may import it.
//
// Both queues use the per-cell sequence number scheme from Dmitry Vyukov's
// bounded MPMC queue:
// https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
package queue

import "sync/atomic"

// SPMC is a bounded single-producer/multi-consumer queue.
//
// The producer appends with Push; any number of consumers take with Pop. Each
// slot carries a sequence number that says whose turn it is to touch it: a
// writer stores the value before publishing the sequence, and a reader observes
// the sequence before reading the value, which makes slot reuse safe.
type SPMC[T any] struct {
	buffer []spmcCell[T]
	mask   uint64
	head   atomic.Uint64 // consumers advance this with CompareAndSwap
	tail   uint64        // producer-only, never shared
}

type spmcCell[T any] struct {
	seq atomic.Uint64
	val T
}

// NewSPMC returns a bounded SPMC queue. Capacity is rounded up to a power of
// two.
func NewSPMC[T any](capacity int) *SPMC[T] {
	size := roundUpPow2(capacity)
	q := &SPMC[T]{buffer: make([]spmcCell[T], size), mask: uint64(size - 1)}
	for i := range q.buffer {
		q.buffer[i].seq.Store(uint64(i))
	}
	return q
}

// Capacity is the queue's fixed capacity.
func (q *SPMC[T]) Capacity() int { return len(q.buffer) }

// Push appends on the producer side. It reports false when the queue is full.
func (q *SPMC[T]) Push(v T) bool {
	pos := q.tail
	cell := &q.buffer[pos&q.mask]
	if cell.seq.Load() != pos {
		return false
	}
	cell.val = v
	cell.seq.Store(pos + 1)
	q.tail = pos + 1
	return true
}

// Pop removes the oldest element. Safe to call from any consumer.
func (q *SPMC[T]) Pop() (T, bool) {
	var zero T
	for {
		pos := q.head.Load()
		cell := &q.buffer[pos&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(pos+1)
		if diff == 0 {
			if q.head.CompareAndSwap(pos, pos+1) {
				value := cell.val
				cell.seq.Store(pos + q.mask + 1)
				return value, true
			}
			continue
		}
		if diff < 0 {
			return zero, false
		}
	}
}

// MPMC is a bounded multi-producer/multi-consumer queue in the style of
// Vyukov. It backs the shared injection queue, tolerating several producers.
type MPMC[T any] struct {
	buffer []mpmcCell[T]
	mask   uint64
	enq    atomic.Uint64
	deq    atomic.Uint64
}

type mpmcCell[T any] struct {
	seq atomic.Uint64
	val T
}

// NewMPMC returns a bounded MPMC queue. Capacity is rounded up to a power of
// two.
func NewMPMC[T any](capacity int) *MPMC[T] {
	size := roundUpPow2(capacity)
	q := &MPMC[T]{buffer: make([]mpmcCell[T], size), mask: uint64(size - 1)}
	for i := range q.buffer {
		q.buffer[i].seq.Store(uint64(i))
	}
	return q
}

// Capacity is the queue's fixed capacity.
func (q *MPMC[T]) Capacity() int { return len(q.buffer) }

// Enqueue reports false when the queue is full.
func (q *MPMC[T]) Enqueue(v T) bool {
	for {
		pos := q.enq.Load()
		cell := &q.buffer[pos&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(pos)
		if diff == 0 {
			if q.enq.CompareAndSwap(pos, pos+1) {
				cell.val = v
				cell.seq.Store(pos + 1)
				return true
			}
			continue
		}
		if diff < 0 {
			return false
		}
	}
}

// Dequeue reports false when the queue is empty.
func (q *MPMC[T]) Dequeue() (T, bool) {
	var zero T
	for {
		pos := q.deq.Load()
		cell := &q.buffer[pos&q.mask]
		seq := cell.seq.Load()
		diff := int64(seq) - int64(pos+1)
		if diff == 0 {
			if q.deq.CompareAndSwap(pos, pos+1) {
				value := cell.val
				cell.val = zero
				cell.seq.Store(pos + q.mask + 1)
				return value, true
			}
			continue
		}
		if diff < 0 {
			return zero, false
		}
	}
}

func roundUpPow2(n int) int {
	size := 1
	for size < n {
		size <<= 1
	}
	if size < 2 {
		size = 2
	}
	return size
}
