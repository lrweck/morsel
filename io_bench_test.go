package morsel

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// writeIOLines writes a CSV-ish file of `lines` rows and returns its path and
// byte size. The write happens before the benchmark timer starts.
func writeIOLines(b *testing.B, lines int) (string, int64) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "data.csv")
	file, err := os.Create(path)
	if err != nil {
		b.Fatal(err)
	}
	writer := bufio.NewWriterSize(file, 1<<20)
	var size int64
	for i := 0; i < lines; i++ {
		n, err := fmt.Fprintf(writer, "%d,%d\n", i, i*2)
		if err != nil {
			b.Fatal(err)
		}
		size += int64(n)
	}
	if err := writer.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
	return path, size
}

// processLine is the per-line work: parse the leading integer plus a dependency
// chain of `rounds` steps, so the workload can be made read-bound or
// compute-bound. It takes a string so it serves both the string and byte paths.
func processLine(s string, rounds int) int64 {
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
	}
	for i := 0; i < rounds; i++ {
		n = n*1103515245 + 12345
	}
	return n & 0xffff
}

func processLineBytes(b []byte, rounds int) int64 {
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
	}
	for i := 0; i < rounds; i++ {
		n = n*1103515245 + 12345
	}
	return n & 0xffff
}

const (
	ioLines  = 1_000_000
	ioLight  = 32
	ioHeavy  = 2048
	ioMorsel = 256
)

func benchSeq(b *testing.B, path string, size int64, rounds int) {
	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		var sum int64
		for scanner.Scan() {
			sum += processLine(string(scanner.Bytes()), rounds)
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		file.Close()
		_ = sum
	}
}

func benchPool(b *testing.B, path string, size int64, rounds int) {
	b.SetBytes(size)
	b.ReportAllocs()
	workers := runtime.GOMAXPROCS(0)
	for b.Loop() {
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		lines := make(chan string, 1024)
		var wg sync.WaitGroup
		var sum atomic.Int64
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var local int64
				for line := range lines {
					local += processLine(line, rounds)
				}
				sum.Add(local)
			}()
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text() // Text allocates a copy; Bytes is reused
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		close(lines)
		wg.Wait()
		file.Close()
		_ = sum.Load()
	}
}

// benchLines reads the file through the Lines adapter: one string per line.
func benchLines(b *testing.B, path string, size int64, rounds, morsel int) {
	b.SetBytes(size)
	b.ReportAllocs()
	ex := NewExecutor(Config{MorselSize: uint(morsel)})
	for b.Loop() {
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		var sum atomic.Int64
		err = Lines(file).ForEachE(func(line string) error {
			sum.Add(processLine(line, rounds))
			return nil
		}, WithExecutor(ex))
		file.Close()
		if err != nil {
			b.Fatal(err)
		}
		_ = sum.Load()
	}
}

// benchChunks reads the file through the Chunks adapter and splits lines inside
// each chunk, so there is no allocation per line (only one copy per chunk). A
// trailing partial line per chunk is skipped; at these chunk sizes that is a
// handful of lines out of a million.
func benchChunks(b *testing.B, path string, size int64, rounds, chunkSize int) {
	b.SetBytes(size)
	b.ReportAllocs()
	ex := NewExecutor(Config{MorselSize: 1})
	for b.Loop() {
		file, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		var sum atomic.Int64
		err = Chunks(file, chunkSize).ForEach(func(chunk []byte) {
			var local int64
			for line := range bytes.Lines(chunk) {
				local += processLineBytes(line, rounds)
			}
			sum.Add(local)
		}, WithExecutor(ex))
		file.Close()
		if err != nil {
			b.Fatal(err)
		}
		_ = sum.Load()
	}
}

// benchStdlibLines is the stdlib-only iterator path: read the whole file, then
// range over bytes.Lines (Go 1.24+). Sequential, but no per-line allocation.
func benchStdlibLines(b *testing.B, path string, size int64, rounds int) {
	b.SetBytes(size)
	b.ReportAllocs()
	for b.Loop() {
		data, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		var sum int64
		for line := range bytes.Lines(data) {
			sum += processLineBytes(line, rounds)
		}
		_ = sum
	}
}

// benchIterStdlib feeds the stdlib iterator (bytes.Lines) into our engine, so
// the same iterator is processed in parallel.
func benchIterStdlib(b *testing.B, path string, size int64, rounds, morsel int) {
	b.SetBytes(size)
	b.ReportAllocs()
	ex := NewExecutor(Config{MorselSize: uint(morsel)})
	for b.Loop() {
		data, err := os.ReadFile(path)
		if err != nil {
			b.Fatal(err)
		}
		var sum atomic.Int64
		err = Iter(bytes.Lines(data)).ForEachE(func(line []byte) error {
			sum.Add(processLineBytes(line, rounds))
			return nil
		}, WithExecutor(ex))
		if err != nil {
			b.Fatal(err)
		}
		_ = sum.Load()
	}
}

// BenchmarkIOLight: cheap per-line work, so the scan and per-line allocation
// dominate.
func BenchmarkIOLight(b *testing.B) {
	path, size := writeIOLines(b, ioLines)
	b.Run("sequential", func(b *testing.B) { benchSeq(b, path, size, ioLight) })
	b.Run("workerpool", func(b *testing.B) { benchPool(b, path, size, ioLight) })
	b.Run("lines", func(b *testing.B) { benchLines(b, path, size, ioLight, ioMorsel) })
	b.Run("stdlib-lines", func(b *testing.B) { benchStdlibLines(b, path, size, ioLight) })
	b.Run("iter-stdlib", func(b *testing.B) { benchIterStdlib(b, path, size, ioLight, ioMorsel) })
	b.Run("chunks/64KiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 64<<10) })
	b.Run("chunks/256KiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 256<<10) })
	b.Run("chunks/1MiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 1<<20) })
}

// BenchmarkIOHeavy: expensive per-line work, so parallelism pays off.
func BenchmarkIOHeavy(b *testing.B) {
	path, size := writeIOLines(b, ioLines)
	b.Run("sequential", func(b *testing.B) { benchSeq(b, path, size, ioHeavy) })
	b.Run("workerpool", func(b *testing.B) { benchPool(b, path, size, ioHeavy) })
	b.Run("lines", func(b *testing.B) { benchLines(b, path, size, ioHeavy, ioMorsel) })
	b.Run("stdlib-lines", func(b *testing.B) { benchStdlibLines(b, path, size, ioHeavy) })
	b.Run("iter-stdlib", func(b *testing.B) { benchIterStdlib(b, path, size, ioHeavy, ioMorsel) })
	b.Run("chunks/64KiB", func(b *testing.B) { benchChunks(b, path, size, ioHeavy, 64<<10) })
	b.Run("chunks/256KiB", func(b *testing.B) { benchChunks(b, path, size, ioHeavy, 256<<10) })
	b.Run("chunks/1MiB", func(b *testing.B) { benchChunks(b, path, size, ioHeavy, 1<<20) })
}

// BenchmarkIOLinesMorselSizes shows the effect of the batch size on the light
// Lines workload.
func BenchmarkIOLinesMorselSizes(b *testing.B) {
	path, size := writeIOLines(b, ioLines)
	for _, morsel := range []int{64, 256, 1024, 4096} {
		b.Run(strconv.Itoa(morsel), func(b *testing.B) {
			benchLines(b, path, size, ioLight, morsel)
		})
	}
}
