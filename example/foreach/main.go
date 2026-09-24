// Command foreach shows the simplest entry point: apply a function to every
// element of a slice in parallel.
//
//	go run ./example/foreach
package main

import (
	"fmt"

	"github.com/lrweck/morsel"
)

type Debt struct {
	Amount int
}

func main() {
	debts := make([]*Debt, 1_000_000)
	for i := range debts {
		debts[i] = &Debt{Amount: i + 1}
	}

	// No options: sane defaults (GOMAXPROCS workers, 256-item morsels).
	err := morsel.ForEach(debts, func(d *Debt) {
		d.Amount *= 2
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("debts=%d first=%d last=%d\n", len(debts), debts[0].Amount, debts[len(debts)-1].Amount)
}
