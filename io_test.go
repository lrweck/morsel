package morsel

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// writeTempFile writes content to a fresh file under t.TempDir and returns its
// path. Real disk I/O, cleaned up by the test framework.
func writeTempFile(t *testing.T, name string, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestChunksRealFile reads a real file from disk in chunks and checks that no
// byte is lost or duplicated: total length and byte sum must match the source.
func TestChunksRealFile(t *testing.T) {
	const size = 2 << 20 // 2 MiB
	content := make([]byte, size)
	var wantSum int64
	for i := range content {
		content[i] = byte(i)
		wantSum += int64(content[i])
	}
	path := writeTempFile(t, "blob.bin", content)

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var total, sum atomic.Int64
	err = Chunks(file, 64<<10).ForEach(func(b []byte) {
		total.Add(int64(len(b)))
		var local int64
		for _, c := range b {
			local += int64(c)
		}
		sum.Add(local)
	}, MorselSize(256))
	if err != nil {
		t.Fatal(err)
	}
	if total.Load() != size {
		t.Fatalf("read %d bytes, want %d", total.Load(), size)
	}
	if sum.Load() != wantSum {
		t.Fatalf("byte sum = %d, want %d", sum.Load(), wantSum)
	}
}

// TestLinesRealFile reads a real text file line by line and sums the numbers,
// comparing against the value computed while writing.
func TestLinesRealFile(t *testing.T) {
	const lines = 100_000
	var sb strings.Builder
	var want int64
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, "%d\n", i)
		want += int64(i)
	}
	path := writeTempFile(t, "numbers.txt", []byte(sb.String()))

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var sum, count atomic.Int64
	err = Lines(file).ForEachE(func(line string) error {
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return err
		}
		sum.Add(n)
		count.Add(1)
		return nil
	}, MorselSize(512))
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != lines {
		t.Fatalf("read %d lines, want %d", count.Load(), lines)
	}
	if sum.Load() != want {
		t.Fatalf("sum = %d, want %d", sum.Load(), want)
	}
}

// TestRowsRealCSV reads a real CSV file through the pull-style Rows adapter and
// sums a column.
func TestRowsRealCSV(t *testing.T) {
	const rows = 50_000
	var sb strings.Builder
	var want int64
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&sb, "%d,%d\n", i, i*2)
		want += int64(i)
	}
	path := writeTempFile(t, "data.csv", []byte(sb.String()))

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	var sum, count atomic.Int64
	err = Rows(func() (int, bool, error) {
		record, err := reader.Read()
		if err == io.EOF {
			return 0, false, nil
		}
		if err != nil {
			return 0, false, err
		}
		n, err := strconv.Atoi(record[0])
		if err != nil {
			return 0, false, err
		}
		return n, true, nil
	}).ForEach(func(n int) {
		sum.Add(int64(n))
		count.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != rows {
		t.Fatalf("read %d rows, want %d", count.Load(), rows)
	}
	if sum.Load() != want {
		t.Fatalf("sum = %d, want %d", sum.Load(), want)
	}
}

// TestLinesRealFileMatchesSequential reads the same file both ways and checks
// the parallel result equals a plain sequential scan.
func TestLinesRealFileMatchesSequential(t *testing.T) {
	const lines = 20_000
	var sb strings.Builder
	for i := 0; i < lines; i++ {
		fmt.Fprintf(&sb, "line-%d-%s\n", i, strings.Repeat("x", i%17))
	}
	path := writeTempFile(t, "mixed.txt", []byte(sb.String()))

	// Sequential reference: total bytes across lines.
	var wantBytes int64
	func() {
		file, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			wantBytes += int64(len(scanner.Text()))
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
	}()

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var gotBytes, count atomic.Int64
	err = Lines(file).ForEach(func(line string) {
		gotBytes.Add(int64(len(line)))
		count.Add(1)
	}, MorselSize(128))
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != lines {
		t.Fatalf("count = %d, want %d", count.Load(), lines)
	}
	if gotBytes.Load() != wantBytes {
		t.Fatalf("bytes = %d, want %d", gotBytes.Load(), wantBytes)
	}
}
