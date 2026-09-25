// Package engine implements the morsel-driven scheduler: bounded per-worker
// queues, work stealing, elastic workers, and completion tracking. It is
// internal: only the parent morsel package may import it.
package engine

import "runtime"

// Config controls a single run. The zero value is valid and normalizes to
// DefaultConfig. The fields are unsigned so a negative value is impossible.
type Config struct {
	// MaxWorkers is a ceiling, not a target. Workers are created on demand
	// and never exceed this count. Zero means GOMAXPROCS.
	MaxWorkers uint
	// MorselSize is the number of items per morsel. Zero means 256.
	MorselSize uint
	// QueueCapacity bounds each worker queue and the injection queue. It is
	// what keeps every queue bounded. Smaller queues fit in cache and measure
	// faster; larger ones buffer more slack for a bursty producer. Zero means 32.
	QueueCapacity uint
	// StealAttempts bounds the steal loop so an idle worker never scans the
	// pool forever. Zero means 4.
	StealAttempts uint
	// RecoverPanics converts a panicking worker into an error instead of
	// crashing the process.
	RecoverPanics bool
	// Eager publishes a partial morsel from an iterator source whenever no
	// work is outstanding — workers would otherwise idle — instead of
	// waiting to fill it to MorselSize. Under load morsels still fill as
	// usual. Slice sources already publish immediately and ignore it.
	Eager bool
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		MaxWorkers:    uint(runtime.GOMAXPROCS(0)),
		MorselSize:    256,
		QueueCapacity: 32,
		StealAttempts: 4,
	}
}

// Normalize fills in defaults for any zero field.
func (c Config) Normalize() Config {
	if c.MaxWorkers == 0 {
		c.MaxWorkers = uint(runtime.GOMAXPROCS(0))
	}
	if c.MorselSize == 0 {
		c.MorselSize = 256
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 32
	}
	if c.StealAttempts == 0 {
		c.StealAttempts = 4
	}
	return c
}
