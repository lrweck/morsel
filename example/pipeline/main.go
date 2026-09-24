// Command pipeline shows the fluent API: a typed chain of stages ended by a
// terminal.
//
//	go run ./example/pipeline
package main

import (
	"fmt"

	"github.com/lrweck/morsel"
)

type Debt struct {
	Amount int
	Valid  bool
}

func main() {
	debts := make([]Debt, 1_000_000)
	for i := range debts {
		debts[i] = Debt{Amount: i + 1, Valid: i%2 == 0}
	}

	total, err := morsel.Slice(debts).
		Filter(func(d Debt) bool { return d.Valid }).
		Map(func(d Debt) int { return d.Amount }).
		Reduce(0,
			func(acc, v int) int { return acc + v },
			func(a, b int) int { return a + b },
		)
	if err != nil {
		panic(err)
	}

	var want int
	for _, d := range debts {
		if d.Valid {
			want += d.Amount
		}
	}
	fmt.Printf("total=%d want=%d equal=%v\n", total, want, total == want)
}
