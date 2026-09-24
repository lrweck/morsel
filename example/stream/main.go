// Command stream reads a file from disk in chunks and processes it in parallel,
// with memory bounded by the chunk size instead of the file size.
//
//	go run ./example/stream <file>
package main

import (
	"bytes"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/lrweck/morsel"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: stream <file>")
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()

	var bytesRead, newlines atomic.Int64
	err = morsel.Chunks(file, 64<<10).ForEach(func(chunk []byte) {
		bytesRead.Add(int64(len(chunk)))
		newlines.Add(int64(bytes.Count(chunk, []byte{'\n'})))
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d bytes, %d newlines\n", bytesRead.Load(), newlines.Load())
}
