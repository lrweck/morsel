package morsel

import (
	"sync/atomic"

	"github.com/lrweck/morsel/internal/engine"
)

// Executor is a reusable engine. It holds only configuration and aggregate
// stats; every run gets its own engine state, so calls never share mutable
// execution state.
type Executor struct {
	cfg   engine.Config
	stats executorStats
}

// executorStats accumulates the counters of every run. Reusable executors are
// meant for many short executions, so each counter is atomic and independent:
// recording a run never blocks another run or a reader.
type executorStats struct {
	morselsCreated  atomic.Uint64
	morselsExecuted atomic.Uint64
	stealsAttempted atomic.Uint64
	stealsSucceeded atomic.Uint64
	workersCreated  atomic.Uint64
}

// NewExecutor returns an Executor. The configuration is normalized: the zero
// value of Config is valid and means "use the defaults". The unsigned fields
// make an invalid configuration impossible, so there is nothing to fail on.
func NewExecutor(cfg Config) *Executor {
	return &Executor{cfg: cfg.engine().Normalize()}
}

// Config returns the executor's normalized configuration.
func (ex *Executor) Config() Config { return configFromEngine(ex.cfg) }

// Stats returns the counters accumulated across every run of this executor.
// The result is a snapshot: each counter is read independently, so a run that
// finishes concurrently may be reflected in some counters and not others. It
// does not promise a transactionally consistent instant across all counters.
func (ex *Executor) Stats() Stats {
	return Stats{
		MorselsCreated:  ex.stats.morselsCreated.Load(),
		MorselsExecuted: ex.stats.morselsExecuted.Load(),
		StealsAttempted: ex.stats.stealsAttempted.Load(),
		StealsSucceeded: ex.stats.stealsSucceeded.Load(),
		WorkersCreated:  ex.stats.workersCreated.Load(),
	}
}

func (ex *Executor) record(s engine.Stats) {
	ex.stats.morselsCreated.Add(s.MorselsCreated)
	ex.stats.morselsExecuted.Add(s.MorselsExecuted)
	ex.stats.stealsAttempted.Add(s.StealsAttempted)
	ex.stats.stealsSucceeded.Add(s.StealsSucceeded)
	ex.stats.workersCreated.Add(s.WorkersCreated)
}

func emptyState[T any]() struct{}               { return struct{}{} }
func emptyMerge[T any](_ *struct{}, _ struct{}) {}
