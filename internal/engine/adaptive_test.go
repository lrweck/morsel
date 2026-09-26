package engine

import (
	"context"
	"testing"
	"time"
)

func adaptiveConfig() Config {
	return Config{
		MaxWorkers:         4,
		MorselSize:         256,
		QueueCapacity:      8,
		StealAttempts:      4,
		AdaptiveMorselSize: true,
		MinMorselSize:      16,
		MaxMorselSize:      1024,
		TargetMorselTime:   time.Millisecond,
	}
}

func newAdaptiveRunner(cfg Config) *Runner[int, struct{}] {
	return NewRunner[int, struct{}](cfg, context.Background(), nil, func() struct{} { return struct{}{} })
}

// TestObserveGrowsOnFastMorsels doubles the size until the cap when morsels
// run well under half the target.
func TestObserveGrowsOnFastMorsels(t *testing.T) {
	r := newAdaptiveRunner(adaptiveConfig().Normalize())
	for range 50 {
		r.observe(time.Microsecond)
	}
	if got := r.MorselSize(); got != 1024 {
		t.Fatalf("MorselSize = %d, want 1024 (grown to max)", got)
	}
}

// TestObserveShrinksOnSlowMorsels halves the size until the floor when morsels
// run well over twice the target.
func TestObserveShrinksOnSlowMorsels(t *testing.T) {
	r := newAdaptiveRunner(adaptiveConfig().Normalize())
	for range 200 {
		r.observe(10 * time.Millisecond)
	}
	if got := r.MorselSize(); got != 16 {
		t.Fatalf("MorselSize = %d, want 16 (shrunk to min)", got)
	}
}

// TestObserveKeepsSizeInBand leaves the size alone while the smoothed time sits
// between half and twice the target.
func TestObserveKeepsSizeInBand(t *testing.T) {
	r := newAdaptiveRunner(adaptiveConfig().Normalize())
	// Fill the EMA with a mid-band value first so a single sample cannot tip it.
	for range 50 {
		r.observe(time.Millisecond)
	}
	before := r.MorselSize()
	for range 50 {
		r.observe(time.Millisecond)
	}
	if got := r.MorselSize(); got != before {
		t.Fatalf("MorselSize = %d, want unchanged %d", got, before)
	}
}

// TestMorselSizeForClamps checks the initial size is clamped into the adaptive
// band and that a non-adaptive run keeps the configured size verbatim.
func TestMorselSizeForClamps(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want uint64
	}{
		{"above max clamps down", Config{AdaptiveMorselSize: true, MorselSize: 100_000, MinMorselSize: 16, MaxMorselSize: 1024}.Normalize(), 1024},
		{"below min clamps up", Config{AdaptiveMorselSize: true, MorselSize: 4, MinMorselSize: 16, MaxMorselSize: 1024}.Normalize(), 16},
		{"inside band unchanged", Config{AdaptiveMorselSize: true, MorselSize: 256, MinMorselSize: 16, MaxMorselSize: 1024}.Normalize(), 256},
		{"disabled keeps size", Config{MorselSize: 100_000}.Normalize(), 100_000},
	}
	for _, tt := range tests {
		if got := morselSizeFor(tt.cfg); got != tt.want {
			t.Errorf("%s: morselSizeFor = %d, want %d", tt.name, got, tt.want)
		}
	}
}

// TestNormalizeAdaptiveDefaults fills the adaptive fields when omitted.
func TestNormalizeAdaptiveDefaults(t *testing.T) {
	cfg := Config{AdaptiveMorselSize: true}.Normalize()
	if cfg.MinMorselSize != 64 || cfg.MaxMorselSize != 8192 || cfg.TargetMorselTime != time.Millisecond {
		t.Fatalf("defaults = min %d max %d target %v, want 64/8192/1ms",
			cfg.MinMorselSize, cfg.MaxMorselSize, cfg.TargetMorselTime)
	}
}

// TestNormalizeSwapsInvertedBounds guards against a min above the max.
func TestNormalizeSwapsInvertedBounds(t *testing.T) {
	cfg := Config{AdaptiveMorselSize: true, MinMorselSize: 4096, MaxMorselSize: 64}.Normalize()
	if cfg.MinMorselSize != 64 || cfg.MaxMorselSize != 4096 {
		t.Fatalf("bounds = %d..%d, want 64..4096", cfg.MinMorselSize, cfg.MaxMorselSize)
	}
}
