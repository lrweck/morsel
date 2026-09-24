// Command iterator feeds a stdlib iterator into the engine. Iter accepts any
// iter.Seq[T], so strings.Lines / bytes.Lines / sql.Rows-style iterators run in
// parallel without a new adapter.
//
//	go run ./example/iterator
package main

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/lrweck/morsel"
)

func main() {
	text := strings.Repeat("the quick brown fox\n", 1_000_000)

	var words atomic.Int64
	err := morsel.Iter(strings.Lines(text)).ForEach(func(line string) {
		words.Add(int64(len(strings.Fields(line))))
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("lines=%d words=%d\n", strings.Count(text, "\n"), words.Load())
}
