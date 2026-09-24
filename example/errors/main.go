// Command errors shows error propagation and context cancellation.
//
//	go run ./example/errors
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/lrweck/morsel"
)

func main() {
	// The first callback error aborts the run and is returned.
	data := make([]int, 1_000_000)
	for i := range data {
		data[i] = i
	}
	err := morsel.ForEachE(data, func(v int) error {
		if v == 42 {
			return errors.New("unlucky element")
		}
		return nil
	})
	fmt.Println("callback error:", err)

	// Cancelling the context stops the run early; the rest is dropped.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var processed atomic.Int64
	err = morsel.ForEach(make([]int, 10_000_000), func(int) {
		if processed.Add(1) == 1000 {
			cancel()
		}
	}, morsel.WithContext(ctx))
	fmt.Printf("context canceled=%v processed=%d\n", errors.Is(err, context.Canceled), processed.Load())
}
