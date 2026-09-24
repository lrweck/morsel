// Command executor shows a reusable Executor and its stats. The executor holds
// only configuration and counters; every run gets its own state, so runs never
// interfere.
//
//	go run ./example/executor
package main

import (
	"context"
	"fmt"

	"github.com/lrweck/morsel"
)

func main() {
	ex := morsel.NewExecutor(morsel.Config{
		MaxWorkers:    8,
		MorselSize:    256,
		QueueCapacity: 256,
		StealAttempts: 4,
	})

	data := make([]int, 1_000_000)
	for i := range data {
		data[i] = i
	}

	for run := 0; run < 3; run++ {
		total, err := ex.ReduceSlice(context.Background(), data, 0,
			func(acc, v int) int { return acc + v },
			func(a, b int) int { return a + b },
		)
		if err != nil {
			panic(err)
		}
		fmt.Printf("run %d: total=%d\n", run, total)
	}

	s := ex.Stats()
	fmt.Printf("morsels created=%d executed=%d, steals=%d/%d, workers=%d\n",
		s.MorselsCreated, s.MorselsExecuted, s.StealsSucceeded, s.StealsAttempted, s.WorkersCreated)
}
