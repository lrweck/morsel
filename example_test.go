package morsel_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/lrweck/morsel"
)

func ExampleForEach() {
	data := []int{1, 2, 3, 4}

	var mu sync.Mutex
	sum := 0
	err := morsel.ForEach(data, func(v int) {
		mu.Lock()
		sum += v
		mu.Unlock()
	}, morsel.MorselSize(2))
	if err != nil {
		panic(err)
	}
	fmt.Println(sum)
	// Output: 10
}

func ExampleMapSlice() {
	data := []int{1, 2, 3}

	out, err := morsel.MapSlice(context.Background(),
		morsel.NewExecutor(morsel.DefaultConfig()),
		data,
		func(v int) int { return v * 2 },
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: [2 4 6]
}

func ExampleReduceSlice() {
	type Debt struct{ Amount int }
	debts := []Debt{{Amount: 10}, {Amount: 20}, {Amount: 30}}

	total, err := morsel.ReduceSlice(context.Background(),
		morsel.NewExecutor(morsel.DefaultConfig()),
		debts, 0,
		func(acc int, d Debt) int { return acc + d.Amount },
		func(a, b int) int { return a + b },
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(total)
	// Output: 60
}

func ExamplePipeline() {
	total, err := morsel.Range(1, 5).
		Map(func(v int) int { return v * v }).
		Reduce(0,
			func(acc, v int) int { return acc + v },
			func(a, b int) int { return a + b },
		)
	if err != nil {
		panic(err)
	}
	fmt.Println(total)
	// Output: 30
}

func ExamplePipeline_Filter() {
	got, err := morsel.Range(0, 10).
		Filter(func(v int) bool { return v%2 == 0 }).
		Collect()
	if err != nil {
		panic(err)
	}
	fmt.Println(len(got))
	// Output: 5
}
