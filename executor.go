package morsel

import (
	"sync"

	"github.com/lrweck/morsel/internal/engine"
)

// Executor is a reusable engine. It holds only configuration and aggregate
// stats; every run gets its own engine state, so calls never share mutable
// execution state.
type Executor struct {
	cfg     engine.Config
	statsMu sync.Mutex
	stats   engine.Stats
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
func (ex *Executor) Stats() Stats {
	ex.statsMu.Lock()
	defer ex.statsMu.Unlock()
	return statsFromEngine(ex.stats)
}

func (ex *Executor) record(s engine.Stats) {
	ex.statsMu.Lock()
	ex.stats.MorselsCreated += s.MorselsCreated
	ex.stats.MorselsExecuted += s.MorselsExecuted
	ex.stats.StealsAttempted += s.StealsAttempted
	ex.stats.StealsSucceeded += s.StealsSucceeded
	ex.stats.WorkersCreated += s.WorkersCreated
	ex.statsMu.Unlock()
}

func emptyState[T any]() struct{}               { return struct{}{} }
func emptyMerge[T any](_ *struct{}, _ struct{}) {}
