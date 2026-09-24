package morsel

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

func TestIterAdapter(t *testing.T) {
	var count atomic.Int64
	err := Iter(func(yield func(int) bool) {
		for i := 0; i < 100; i++ {
			if !yield(i) {
				return
			}
		}
	}).ForEach(func(int) { count.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 100 {
		t.Fatalf("count = %d, want 100", count.Load())
	}
}

func TestIterErrAdapter(t *testing.T) {
	boom := errors.New("iter")
	err := IterErr(func(yield func(int, error) bool) {
		if !yield(1, nil) {
			return
		}
		yield(0, boom)
	}).ForEach(func(int) {})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

type errReader struct {
	data []byte
	err  error
	done bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	return copy(p, r.data), r.err
}

func TestChunksReadError(t *testing.T) {
	boom := errors.New("read")
	err := Chunks(&errReader{data: []byte("hello"), err: boom}, 2).ForEach(func([]byte) {})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestLinesScannerError(t *testing.T) {
	long := strings.Repeat("x", 2<<20) // exceeds bufio.MaxScanTokenSize
	err := Lines(strings.NewReader(long)).ForEach(func(string) {})
	if err == nil {
		t.Fatal("want a scanner error")
	}
}

func TestRowsError(t *testing.T) {
	boom := errors.New("rows")
	i := 0
	next := func() (int, bool, error) {
		i++
		if i == 3 {
			return 0, false, boom
		}
		return i, true, nil
	}
	err := Rows(next).ForEach(func(int) {})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}

func TestPipelineNilFunctions(t *testing.T) {
	if err := Slice([]int{1}).Map[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilFunction) {
		t.Fatal("Map")
	}
	if err := Slice([]int{1}).MapE[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilFunction) {
		t.Fatal("MapE")
	}
	if err := Slice([]int{1}).Filter(nil).ForEach(func(int) {}); !errors.Is(err, ErrNilFunction) {
		t.Fatal("Filter")
	}
	if err := Slice([]int{1}).FlatMap[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilFunction) {
		t.Fatal("FlatMap")
	}
	if err := Slice([]int{1}).ForEach(nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEach")
	}
	if err := Slice([]int{1}).ForEachE(nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("ForEachE")
	}
	if _, err := Slice([]int{1}).Reduce(0, nil, nil); !errors.Is(err, ErrNilFunction) {
		t.Fatal("Reduce")
	}
}

func TestNilSources(t *testing.T) {
	if err := Iter[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("Iter")
	}
	if err := IterErr[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("IterErr")
	}
	if err := From[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("From")
	}
	if err := Chunks(nil, 10).ForEach(func([]byte) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("Chunks")
	}
	if err := Lines(nil).ForEach(func(string) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("Lines")
	}
	if err := Rows[int](nil).ForEach(func(int) {}); !errors.Is(err, ErrNilSource) {
		t.Fatal("Rows")
	}
}

func TestPipelineReduceDifferentAccumulator(t *testing.T) {
	type debt struct{ amount int }
	debts := []debt{{10}, {20}, {30}, {40}}
	total, err := Slice(debts).Reduce(0,
		func(acc int, d debt) int { return acc + d.amount },
		func(a, b int) int { return a + b },
		MorselSize(1))
	if err != nil {
		t.Fatal(err)
	}
	if total != 100 {
		t.Fatalf("total = %d, want 100", total)
	}
}

func TestPipelineCollectCount(t *testing.T) {
	got, err := Range(0, 1000).Collect(MorselSize(7))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1000 {
		t.Fatalf("len = %d, want 1000", len(got))
	}
}

func TestPipelineContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Slice([]int{1, 2, 3}).ForEach(func(int) {}, WithContext(ctx)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Slice: err = %v, want context.Canceled", err)
	}
	if err := Range(0, 10).ForEach(func(int) {}, WithContext(ctx)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Range: err = %v, want context.Canceled", err)
	}
	if _, err := Range(0, 10).Collect(WithContext(ctx)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Collect: err = %v, want context.Canceled", err)
	}
}

func TestFilterRemovesAll(t *testing.T) {
	got, err := Range(0, 10).Filter(func(int) bool { return false }).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestFlatMapEmpty(t *testing.T) {
	got, err := Range(0, 10).FlatMap(func(int) []int { return nil }).Collect()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestPipelineForEachEPropagates(t *testing.T) {
	boom := errors.New("boom")
	err := Range(0, 100).ForEachE(func(v int) error {
		if v == 42 {
			return boom
		}
		return nil
	}, MorselSize(1))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
}
