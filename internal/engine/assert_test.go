package engine

import "testing"

func TestAssertPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("assert(false) did not panic")
		}
		failed, ok := r.(assertionError)
		if !ok {
			t.Fatalf("panic value = %T, want assertionError", r)
		}
		if failed.Error() == "" {
			t.Fatal("assertionError.Error() is empty")
		}
	}()
	assert(false, "boom")
}

func TestAssertOK(t *testing.T) {
	assert(true, "never fires")
}
