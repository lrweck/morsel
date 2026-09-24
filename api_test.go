package morsel

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func testExecutor() *Executor {
	return NewExecutor(Config{MaxWorkers: 4, MorselSize: 8, QueueCapacity: 64, StealAttempts: 4})
}

func TestForEachSliceSum(t *testing.T) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	ex := testExecutor()
	var sum atomic.Int64
	if err := ex.ForEachSlice(context.Background(), data, func(v int) error {
		sum.Add(int64(v))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := int64(999 * 1000 / 2); sum.Load() != want {
		t.Fatalf("sum = %d, want %d", sum.Load(), want)
	}
	if ex.Stats().MorselsExecuted == 0 {
		t.Fatal("no morsels executed")
	}
}

func TestForEachSliceEmpty(t *testing.T) {
	ex := testExecutor()
	if err := ex.ForEachSlice(context.Background(), nil, func(int) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestForEachSeq(t *testing.T) {
	ex := testExecutor()
	var count atomic.Int64
	err := ex.ForEachSeq(context.Background(), func(yield func(int) bool) {
		for i := range 1000 {
			if !yield(i) {
				return
			}
		}
	}, func(int) error { count.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1000 {
		t.Fatalf("count = %d, want 1000", count.Load())
	}
}

func TestForEachErrorAborts(t *testing.T) {
	boom := errors.New("boom")
	ex := testExecutor()
	data := make([]int, 1000)
	var seen atomic.Int64
	err := ex.ForEachSlice(context.Background(), data, func(int) error {
		if seen.Add(1) == 1 {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestForEachContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ex := testExecutor()
	err := ex.ForEachSlice(ctx, []int{1, 2, 3}, func(int) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestPanicRecovered(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 2, MorselSize: 1, QueueCapacity: 8, RecoverPanics: true})
	err := ex.ForEachSlice(context.Background(), []int{1, 2, 3}, func(int) error { panic("kaboom") })
	if err == nil || !strings.Contains(err.Error(), "kaboom") {
		t.Fatalf("err = %v, want recovered panic", err)
	}
}

func TestMapSlicePreservesOrder(t *testing.T) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	ex := testExecutor()
	got, err := ex.MapSlice(context.Background(), data, func(v int) int { return v * 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(data) {
		t.Fatalf("len = %d, want %d", len(got), len(data))
	}
	for i, v := range got {
		if v != i*2 {
			t.Fatalf("got[%d] = %d, want %d", i, v, i*2)
		}
	}
}

func TestReduceSlice(t *testing.T) {
	data := make([]int, 1000)
	for i := range data {
		data[i] = i
	}
	ex := testExecutor()
	sum, err := ex.ReduceSlice(context.Background(), data, 0,
		func(acc, v int) int { return acc + v },
		func(a, b int) int { return a + b })
	if err != nil {
		t.Fatal(err)
	}
	if want := 999 * 1000 / 2; sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
}

func TestReduceSliceDifferentAccumulator(t *testing.T) {
	type debt struct{ amount int }
	data := make([]debt, 1000)
	for i := range data {
		data[i] = debt{amount: i + 1}
	}
	ex := testExecutor()
	total, err := ex.ReduceSlice(context.Background(), data, 0,
		func(acc int, d debt) int { return acc + d.amount },
		func(a, b int) int { return a + b })
	if err != nil {
		t.Fatal(err)
	}
	if want := 1000 * 1001 / 2; total != want {
		t.Fatalf("total = %d, want %d", total, want)
	}
}

func TestExecutorReuse(t *testing.T) {
	ex := testExecutor()
	for run := range 5 {
		var sum atomic.Int64
		if err := ex.ForEachSlice(context.Background(), []int{1, 2, 3}, func(v int) error {
			sum.Add(int64(v))
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if sum.Load() != 6 {
			t.Fatalf("run %d: sum = %d, want 6", run, sum.Load())
		}
	}
}

// TestNoLostOrDuplicatedMorsels runs a large workload and checks every element
// is processed exactly once, across worker counts both below and above the
// workload size.
func TestNoLostOrDuplicatedMorsels(t *testing.T) {
	const total = 100_000
	data := make([]int, total)
	for i := range data {
		data[i] = i
	}
	for _, workers := range []int{1, 2, 4, 8} {
		for _, morsel := range []int{1, 7, 256} {
			ex := NewExecutor(Config{
				MaxWorkers: uint(workers), MorselSize: uint(morsel), QueueCapacity: 32, StealAttempts: 4,
			})
			seen := make([]atomic.Int32, total)
			err := ex.ForEachSlice(context.Background(), data, func(v int) error {
				seen[v].Add(1)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for i := range seen {
				if got := seen[i].Load(); got != 1 {
					t.Fatalf("workers=%d morsel=%d: element %d seen %d times, want 1",
						workers, morsel, i, got)
				}
			}
		}
	}
}

// TestWorkDistributed checks the pool really uses more than one worker for a
// large workload, which is the observable effect of work stealing.
func TestWorkDistributed(t *testing.T) {
	ex := NewExecutor(Config{MaxWorkers: 8, MorselSize: 1, QueueCapacity: 64, StealAttempts: 8})
	var mu sync.Mutex
	current, peak := 0, 0
	err := ex.ForEachSlice(context.Background(), make([]int, 4096), func(int) error {
		mu.Lock()
		current++
		if current > peak {
			peak = current
		}
		mu.Unlock()
		for i := range 1000 {
			_ = i * i
		}
		mu.Lock()
		current--
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if peak < 2 {
		t.Fatalf("peak concurrency = %d, want >= 2", peak)
	}
	if ex.Stats().WorkersCreated < 2 {
		t.Fatalf("workers created = %d, want >= 2", ex.Stats().WorkersCreated)
	}
	if ex.Stats().StealsSucceeded == 0 {
		t.Fatalf("no successful steals in an irregular workload; stats = %+v", ex.Stats())
	}
}

func TestPipelineMapReduce(t *testing.T) {
	type debt struct{ amount int }
	debts := make([]debt, 1000)
	for i := range debts {
		debts[i] = debt{amount: i + 1}
	}
	total, err := Slice(debts).
		Map(func(d debt) int { return d.amount * 3 }).
		Reduce(0, func(acc, v int) int { return acc + v }, func(a, b int) int { return a + b },
			MorselSize(16))
	if err != nil {
		t.Fatal(err)
	}
	want := 0
	for i := 1; i <= 1000; i++ {
		want += i * 3
	}
	if total != want {
		t.Fatalf("total = %d, want %d", total, want)
	}
}

func TestPipelineFilterFlatMapCollect(t *testing.T) {
	got, err := Range(0, 100).
		Filter(func(v int) bool { return v%2 == 0 }).
		FlatMap(func(v int) []int { return []int{v, v} }).
		Collect(MorselSize(8))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("len = %d, want 100", len(got))
	}
	sort.Ints(got)
	sum, want := 0, 0
	for _, v := range got {
		sum += v
	}
	for v := 0; v < 100; v += 2 {
		want += 2 * v
	}
	if sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
}

func TestPipelineMapE(t *testing.T) {
	boom := errors.New("bad")
	_, err := Range(0, 100).
		MapE(func(v int) (int, error) {
			if v == 42 {
				return 0, boom
			}
			return v, nil
		}).
		Collect(MorselSize(1))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestPipelineFromSource(t *testing.T) {
	var count atomic.Int64
	err := From(func(yield func(Batch[int]) bool) error {
		for i := range 10 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return nil
			}
		}
		return nil
	}).ForEach(func(int) { count.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 10 {
		t.Fatalf("count = %d, want 10", count.Load())
	}
}

func TestSourceError(t *testing.T) {
	boom := errors.New("source failed")
	err := From(func(yield func(Batch[int]) bool) error {
		yield(Batch[int]{Items: []int{1}})
		return boom
	}).ForEach(func(int) {})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestChunks(t *testing.T) {
	var total atomic.Int64
	err := Chunks(strings.NewReader("hello world"), 3).ForEach(func(b []byte) {
		total.Add(int64(len(b)))
	})
	if err != nil {
		t.Fatal(err)
	}
	if total.Load() != 11 {
		t.Fatalf("read %d bytes, want 11", total.Load())
	}
}

func TestLines(t *testing.T) {
	var count atomic.Int64
	err := Lines(strings.NewReader("a\nb\nc\n")).ForEach(func(string) { count.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 3 {
		t.Fatalf("lines = %d, want 3", count.Load())
	}
}

func TestRows(t *testing.T) {
	rows := [][]int{{1, 2}, {3, 4}, {5, 6}}
	i := 0
	next := func() (int, bool, error) {
		if i >= len(rows) {
			return 0, false, nil
		}
		v := rows[i][0] + rows[i][1]
		i++
		return v, true, nil
	}
	got, err := Rows(next).Collect(MorselSize(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("rows = %d, want 3", len(got))
	}
}

func TestMapSeq(t *testing.T) {
	ex := testExecutor()
	seq := func(yield func(int) bool) {
		for i := range 100 {
			if !yield(i) {
				return
			}
		}
	}
	got, err := ex.MapSeq(context.Background(), seq, func(v int) int { return v * 2 })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("len = %d, want 100", len(got))
	}
	sort.Ints(got)
	for i, v := range got {
		if v != i*2 {
			t.Fatalf("got[%d] = %d, want %d", i, v, i*2)
		}
	}
}

func TestReduceSeq(t *testing.T) {
	ex := testExecutor()
	seq := func(yield func(int) bool) {
		for i := 1; i <= 100; i++ {
			if !yield(i) {
				return
			}
		}
	}
	sum, err := ex.ReduceSeq(context.Background(), seq, 0,
		func(acc, v int) int { return acc + v },
		func(a, b int) int { return a + b })
	if err != nil {
		t.Fatal(err)
	}
	if want := 100 * 101 / 2; sum != want {
		t.Fatalf("sum = %d, want %d", sum, want)
	}
}

func TestForEachDefaults(t *testing.T) {
	var count atomic.Int64
	if err := ForEach([]int{1, 2, 3}, func(int) { count.Add(1) }); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 3 {
		t.Fatalf("count = %d, want 3", count.Load())
	}
	if got := DefaultConfig(); got.MaxWorkers <= 0 || got.MorselSize <= 0 {
		t.Fatalf("insane defaults: %+v", got)
	}
}

func TestForEachErgonomicOptions(t *testing.T) {
	data := make([]int, 1000)
	var sum atomic.Int64
	err := ForEach(data, func(v int) { sum.Add(int64(v)) }, MorselSize(1), MaxWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	if sum.Load() != 0 {
		t.Fatalf("sum = %d, want 0", sum.Load())
	}
}

func TestForEachE(t *testing.T) {
	boom := errors.New("boom")
	err := ForEachE([]int{1, 2, 3}, func(v int) error {
		if v == 2 {
			return boom
		}
		return nil
	}, MorselSize(1))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestForEachMissingFunction(t *testing.T) {
	if err := ForEach([]int{1}, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatalf("err = %v, want ErrNilFunction", err)
	}
}

func TestNilIterator(t *testing.T) {
	ex := testExecutor()
	err := ex.ForEachSeq(context.Background(), nil, func(int) error { return nil })
	if !errors.Is(err, ErrNilSource) {
		t.Fatalf("err = %v, want ErrNilSource", err)
	}
}

func TestIterErrPropagates(t *testing.T) {
	boom := errors.New("iter failed")
	ex := testExecutor()
	err := ex.ForEachSeqErr(context.Background(), func(yield func(int, error) bool) {
		if !yield(1, nil) {
			return
		}
		yield(0, boom)
	}, func(int) error { return nil })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestWithExecutor(t *testing.T) {
	ex := testExecutor()
	var count atomic.Int64
	err := ForEach([]int{1, 2, 3}, func(int) { count.Add(1) }, WithExecutor(ex))
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 3 {
		t.Fatalf("count = %d, want 3", count.Load())
	}
}
