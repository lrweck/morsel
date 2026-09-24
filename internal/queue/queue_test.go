package queue

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestSPMCFIFO(t *testing.T) {
	q := NewSPMC[int](8)
	for i := 1; i <= 5; i++ {
		if !q.Push(i) {
			t.Fatalf("Push %d failed", i)
		}
	}
	for want := 1; want <= 5; want++ {
		got, ok := q.Pop()
		if !ok || got != want {
			t.Fatalf("Pop = %d,%v, want %d,true", got, ok, want)
		}
	}
	if _, ok := q.Pop(); ok {
		t.Fatal("Pop on empty queue succeeded")
	}
}

func TestSPMCBounded(t *testing.T) {
	q := NewSPMC[int](4)
	capacity := q.Capacity()
	for i := 0; i < capacity; i++ {
		if !q.Push(i) {
			t.Fatalf("Push %d failed before capacity", i)
		}
	}
	if q.Push(999) {
		t.Fatal("Push past capacity succeeded")
	}
	if _, ok := q.Pop(); !ok {
		t.Fatal("Pop failed")
	}
	if !q.Push(999) {
		t.Fatal("Push after free failed")
	}
}

// TestSPMCLastElementRace drives the consumer race on the final element with a
// deterministic barrier: exactly one consumer must win.
func TestSPMCLastElementRace(t *testing.T) {
	q := NewSPMC[int](8)
	for i := 0; i < 20_000; i++ {
		if !q.Push(i) {
			t.Fatal("Push failed")
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		var aOK, bOK bool
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _, aOK = q.Pop() }()
		go func() { defer wg.Done(); <-start; _, bOK = q.Pop() }()
		close(start)
		wg.Wait()

		winners := 0
		if aOK {
			winners++
		}
		if bOK {
			winners++
		}
		if winners != 1 {
			t.Fatalf("iteration %d: a=%v b=%v, want exactly one winner", i, aOK, bOK)
		}
	}
}

// TestSPMCNoLostOrDuplicatedItems has the producer produce while thieves drain.
// Every value must be observed exactly once.
func TestSPMCNoLostOrDuplicatedItems(t *testing.T) {
	const total = 200_000
	const consumers = 3

	q := NewSPMC[int](64)
	var mu sync.Mutex
	seen := make(map[int]bool, total)
	collected := 0

	record := func(v int) {
		mu.Lock()
		if seen[v] {
			mu.Unlock()
			t.Errorf("value %d collected twice", v)
			return
		}
		seen[v] = true
		collected++
		mu.Unlock()
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if v, ok := q.Pop(); ok {
					record(v)
					continue
				}
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}

	for i := 0; i < total; i++ {
		for !q.Push(i) {
			if v, ok := q.Pop(); ok {
				record(v)
			}
		}
	}
	for {
		v, ok := q.Pop()
		if !ok {
			break
		}
		record(v)
	}
	close(stop)
	wg.Wait()

	if collected != total {
		t.Fatalf("collected %d values, want %d", collected, total)
	}
}

func TestMPMCFIFO(t *testing.T) {
	q := NewMPMC[int](8)
	for i := 1; i <= 5; i++ {
		if !q.Enqueue(i) {
			t.Fatalf("Enqueue %d failed", i)
		}
	}
	for want := 1; want <= 5; want++ {
		got, ok := q.Dequeue()
		if !ok || got != want {
			t.Fatalf("Dequeue = %d,%v, want %d,true", got, ok, want)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("Dequeue on empty queue succeeded")
	}
}

func TestMPMCBounded(t *testing.T) {
	q := NewMPMC[int](4)
	capacity := q.Capacity()
	for i := 0; i < capacity; i++ {
		if !q.Enqueue(i) {
			t.Fatalf("Enqueue %d failed before capacity", i)
		}
	}
	if q.Enqueue(999) {
		t.Fatal("Enqueue past capacity succeeded")
	}
	if _, ok := q.Dequeue(); !ok {
		t.Fatal("Dequeue failed")
	}
	if !q.Enqueue(999) {
		t.Fatal("Enqueue after free failed")
	}
}

// TestMPMCConcurrent runs several producers and consumers and checks that every
// value is delivered exactly once.
func TestMPMCConcurrent(t *testing.T) {
	const producers = 4
	const consumers = 4
	const perProducer = 50_000
	const total = producers * perProducer

	q := NewMPMC[int](256)
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int]bool, total)

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				v := p*perProducer + i
				for !q.Enqueue(v) {
				}
			}
		}(p)
	}

	var done sync.WaitGroup
	done.Add(consumers)
	var collected atomic.Int64
	for c := 0; c < consumers; c++ {
		go func() {
			defer done.Done()
			for {
				v, ok := q.Dequeue()
				if !ok {
					if collected.Load() >= total {
						return
					}
					continue
				}
				mu.Lock()
				if seen[v] {
					mu.Unlock()
					t.Errorf("value %d collected twice", v)
					return
				}
				seen[v] = true
				mu.Unlock()
				collected.Add(1)
			}
		}()
	}

	wg.Wait()
	done.Wait()

	if collected.Load() != total {
		t.Fatalf("collected %d, want %d", collected.Load(), total)
	}
}
