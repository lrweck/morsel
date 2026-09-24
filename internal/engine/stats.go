package engine

// Stats is a snapshot of the work one run performed. Counters are gathered per
// worker and merged at the end, so collecting them costs no shared lock on the
// morsel path.
type Stats struct {
	MorselsCreated  uint64
	MorselsExecuted uint64
	StealsAttempted uint64
	StealsSucceeded uint64
	WorkersCreated  uint64
}

// WorkerStats are a worker's private counters. They are touched only by their
// owner and merged into Stats after the run.
type WorkerStats struct {
	MorselsExecuted uint64
	StealsAttempted uint64
	StealsSucceeded uint64
}
