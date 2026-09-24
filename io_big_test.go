package morsel

import (
	"os"
	"testing"
)

// Optional big-file I/O benchmark. The benchmarks in io_bench_test.go generate
// a 1M-line (~14 MB) file, which is fast to reproduce but small enough that the
// page cache absorbs it and the numbers come out optimistic. Point MORSEL_IO_FILE
// at a large file to run the same comparisons at scale, where the disk is the
// limit:
//
//	MORSEL_IO_FILE=/tmp/data-5gb.csv go test -run='^$' -bench=BigIO -benchmem .
//
// Any file with one integer-prefixed record per line works; the CSV the other
// benchmarks write ("<i>,<2i>") is one such file, and 1 GiB is ~67M lines.
//
// To compare a cold disk read against a warm one, drop the file's page cache
// between runs (no root needed) and take a single timed iteration:
//
//	dd if=$MORSEL_IO_FILE of=/dev/null bs=4M iflag=nocache   # evict
//	go test -run='^$' -bench=BigIO -benchtime=1x -count=1 .  # cold
//	dd if=$MORSEL_IO_FILE of=/dev/null bs=4M                # warm it
//	go test -run='^$' -bench=BigIO -benchtime=1x -count=1 .  # warm
//
// Use an exact -bench pattern (one benchmark per run): the sub-benchmarks run in
// sequence inside a single invocation, so the first one warms the cache for the
// rest. Only the light-work variant is defined here: the heavy one would spend
// minutes per iteration on a file this size.
func bigIOFile(b *testing.B) (string, int64) {
	b.Helper()
	path := os.Getenv("MORSEL_IO_FILE")
	if path == "" {
		b.Skip("MORSEL_IO_FILE not set")
	}
	st, err := os.Stat(path)
	if err != nil {
		b.Fatal(err)
	}
	return path, st.Size()
}

func BenchmarkBigIOLight(b *testing.B) {
	path, size := bigIOFile(b)
	b.Run("sequential", func(b *testing.B) { benchSeq(b, path, size, ioLight) })
	b.Run("channel-pool", func(b *testing.B) { benchPool(b, path, size, ioLight) })
	b.Run("lines", func(b *testing.B) { benchLines(b, path, size, ioLight, ioMorsel) })
	// stdlib-lines and iter-stdlib read the whole file into memory, so they only
	// make sense while the file fits comfortably in RAM.
	if size <= 2<<30 {
		b.Run("stdlib-lines", func(b *testing.B) { benchStdlibLines(b, path, size, ioLight) })
		b.Run("iter-stdlib", func(b *testing.B) { benchIterStdlib(b, path, size, ioLight, ioMorsel) })
	}
	b.Run("chunks/64KiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 64<<10) })
	b.Run("chunks/256KiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 256<<10) })
	b.Run("chunks/1MiB", func(b *testing.B) { benchChunks(b, path, size, ioLight, 1<<20) })
	b.Run("chunkspooled/64KiB", func(b *testing.B) { benchChunksPooled(b, path, size, ioLight, 64<<10) })
	b.Run("chunkspooled/256KiB", func(b *testing.B) { benchChunksPooled(b, path, size, ioLight, 256<<10) })
}
