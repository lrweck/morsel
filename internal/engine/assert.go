package engine

// assertionError marks a failed invariant check. The distinct type lets panic
// recovery tell a corrupt-internal-state panic apart from a user panic.
type assertionError string

func (e assertionError) Error() string { return string(e) }

// assert panics when condition is false.
//
// Assertions guard programmer errors — broken invariants in the pool's
// bookkeeping — not operating errors, which are returned to the caller.
// Crashing is the only correct response to a corrupt invariant, because
// continuing would turn a correctness bug into silent data loss. Assertion
// panics are never swallowed, even when RecoverPanics is enabled.
func assert(condition bool, message string) {
	if !condition {
		panic(assertionError("morsel: assertion failed: " + message))
	}
}
