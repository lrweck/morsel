package morsel

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/lrweck/morsel/internal/engine"
)

func TestRangeSize(t *testing.T) {
	tests := []struct {
		name   string
		start  int
		end    int
		want   int
		wantOK bool
	}{
		{"zero", 0, 0, 0, true},
		{"equal", 7, 7, 0, true},
		{"adjacent", 0, 1, 1, true},
		{"negative adjacent", -1, 0, 1, true},
		{"crossing zero", -5, 5, 10, true},
		{"reversed", 5, 3, 0, false},
		{"up to max", 0, math.MaxInt, math.MaxInt, true},
		{"max tail", math.MaxInt - 1, math.MaxInt, 1, true},
		{"min head", math.MinInt, math.MinInt + 1, 1, true},
		{"min to zero", math.MinInt, 0, 0, false},
		{"full range", math.MinInt, math.MaxInt, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := rangeSize(tt.start, tt.end)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("rangeSize(%d, %d) = (%d, %v), want (%d, %v)",
					tt.start, tt.end, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestNextRangeEnd(t *testing.T) {
	tests := []struct {
		name string
		lo   int
		end  int
		size uint
		want int
	}{
		{"mid", 0, 100, 8, 8},
		{"exact boundary", 92, 100, 8, 100},
		{"tail shorter than size", 96, 100, 8, 100},
		{"crossing zero", -5, 5, 4, -1},
		{"single below max", math.MaxInt - 1, math.MaxInt, 8, math.MaxInt},
		{"overflowing max tail", math.MaxInt - 3, math.MaxInt, 8, math.MaxInt},
		{"min head", math.MinInt, math.MinInt + 1, 8, math.MinInt + 1},
		{"full range first step", math.MinInt, math.MaxInt, 4, math.MinInt + 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := nextRangeEnd(tt.lo, tt.end, tt.size)
			if got != tt.want {
				t.Fatalf("nextRangeEnd(%d, %d, %d) = %d, want %d",
					tt.lo, tt.end, tt.size, got, tt.want)
			}
			if got <= tt.lo || got > tt.end {
				t.Fatalf("result %d outside (%d, %d]", got, tt.lo, tt.end)
			}
		})
	}
}

func TestRangeBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		start int
		end   int
		want  []int
	}{
		{"empty", 3, 3, nil},
		{"reversed", 5, 3, nil},
		{"adjacent", 4, 5, []int{4}},
		{"crossing zero", -3, 3, []int{-3, -2, -1, 0, 1, 2}},
		{"negative only", -3, -1, []int{-3, -2}},
		{"min head", math.MinInt, math.MinInt + 2, []int{math.MinInt, math.MinInt + 1}},
		{"max tail", math.MaxInt - 2, math.MaxInt, []int{math.MaxInt - 2, math.MaxInt - 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Range(tt.start, tt.end).Collect(MorselSize(2), MaxWorkers(1))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("Range(%d, %d) = %v, want %v", tt.start, tt.end, got, tt.want)
			}
		})
	}
}

// recordingPublisher keeps the first limit morsels and then stops the feed, so
// a huge range can be split and inspected without executing every element.
type recordingPublisher[T any] struct {
	limit int
	size  uint
	got   [][]T
}

func (p *recordingPublisher[T]) Publish(m engine.Work[T]) bool {
	if p.limit > 0 && len(p.got) >= p.limit {
		return false
	}
	p.got = append(p.got, slices.Clone(m.Items))
	return true
}

func (p *recordingPublisher[T]) Pending() int64 { return 0 }

func (p *recordingPublisher[T]) MorselSize() uint { return p.size }

// TestRangeExtremeSplitsWithoutExecuting constructs and splits the full
// [MinInt, MaxInt) range. The publisher stops after two morsels, so only eight
// elements are ever built; the rest of the range is never executed.
func TestRangeExtremeSplitsWithoutExecuting(t *testing.T) {
	ex := NewExecutor(Config{MorselSize: 4})
	pub := &recordingPublisher[int]{limit: 2, size: 4}
	p := Range(math.MinInt, math.MaxInt)
	if p.size != -1 {
		t.Fatalf("size = %d, want -1 for an unrepresentable range", p.size)
	}
	if err := p.feed(ex, context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	if len(pub.got) != 2 {
		t.Fatalf("morsels = %d, want 2", len(pub.got))
	}
	want := [][]int{
		{math.MinInt, math.MinInt + 1, math.MinInt + 2, math.MinInt + 3},
		{math.MinInt + 4, math.MinInt + 5, math.MinInt + 6, math.MinInt + 7},
	}
	for i := range want {
		if !slices.Equal(pub.got[i], want[i]) {
			t.Fatalf("morsel %d = %v, want %v", i, pub.got[i], want[i])
		}
	}
}

// TestRangeMaxIntSplits guards the lo+size overflow at the top of the int range.
func TestRangeMaxIntSplits(t *testing.T) {
	ex := NewExecutor(Config{MorselSize: 4})
	pub := &recordingPublisher[int]{size: 4}
	if err := Range(math.MaxInt-6, math.MaxInt).feed(ex, context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	got := slices.Concat(pub.got...)
	want := []int{math.MaxInt - 6, math.MaxInt - 5, math.MaxInt - 4, math.MaxInt - 3, math.MaxInt - 2, math.MaxInt - 1}
	if !slices.Equal(got, want) {
		t.Fatalf("got = %v, want %v", got, want)
	}
}
