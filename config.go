package morsel

import "github.com/lrweck/morsel/internal/engine"

// Config controls a single run. The zero value is valid and normalizes to
// [DefaultConfig]. The fields are unsigned so a negative value is impossible.
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
	// RecoverPanics turns a panicking callback into an error instead of
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
	return configFromEngine(engine.DefaultConfig())
}

// Stats is a snapshot of the work an Executor has performed. Counters are
// gathered per worker and merged at the end of a run, so collecting them costs
// no shared lock on the morsel path.
type Stats struct {
	// MorselsCreated is the number of morsels published.
	MorselsCreated uint64
	// MorselsExecuted is the number of morsels processed.
	MorselsExecuted uint64
	// StealsAttempted is the number of steal probes across all workers.
	StealsAttempted uint64
	// StealsSucceeded is the number of morsels taken by stealing.
	StealsSucceeded uint64
	// WorkersCreated is the number of worker goroutines started.
	WorkersCreated uint64
}

func (c Config) engine() engine.Config {
	return engine.Config{
		MaxWorkers:    c.MaxWorkers,
		MorselSize:    c.MorselSize,
		QueueCapacity: c.QueueCapacity,
		StealAttempts: c.StealAttempts,
		RecoverPanics: c.RecoverPanics,
		Eager:         c.Eager,
	}
}

func configFromEngine(c engine.Config) Config {
	return Config{
		MaxWorkers:    c.MaxWorkers,
		MorselSize:    c.MorselSize,
		QueueCapacity: c.QueueCapacity,
		StealAttempts: c.StealAttempts,
		RecoverPanics: c.RecoverPanics,
		Eager:         c.Eager,
	}
}

func statsFromEngine(s engine.Stats) Stats {
	return Stats{
		MorselsCreated:  s.MorselsCreated,
		MorselsExecuted: s.MorselsExecuted,
		StealsAttempted: s.StealsAttempted,
		StealsSucceeded: s.StealsSucceeded,
		WorkersCreated:  s.WorkersCreated,
	}
}
