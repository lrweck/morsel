# morsel

**Morsel-driven** parallelism for Go 1.27: work is split into *morsels*
(fixed-size batches), handed to an **elastic** worker pool with **work
stealing**, **bounded** queues, and **zero dependencies**. Based on Leis et al.,
*Morsel-Driven Parallelism* (SIGMOD 2014).

- **Bounded by default**: no queue grows without limit; backpressure is explicit.
- **Elastic**: workers spin up on demand and stop when the work runs out.
- **Work stealing**: an idle worker steals from its peers — great for irregular work.
- **Zero dependencies**: standard library only.
- **Generic**: slices, ranges, `iter.Seq`, readers, and rows.
- **Cancellation and errors** via `context.Context`; the first failure aborts the run.

<p align="center">
  <picture>
    <source srcset="docs/morsel.svg" type="image/svg+xml">
    <img src="docs/morsel.png" width="1000"
         alt="morsels flow from a source into per-worker queues; busy workers light up and an idle worker steals from a busy one">
  </picture>
</p>

## Contents

- [Install](#install)
- [Getting started](#getting-started)
- [When to use it — and when not to](#when-to-use-it--and-when-not-to)
- [Compared to traditional Go](#compared-to-traditional-go)
- [Pipeline](#pipeline)
- [Adapters (sources)](#adapters-sources)
- [Streaming and resource usage](#streaming-and-resource-usage)
- [Errors and cancellation](#errors-and-cancellation)
- [Reusable executor and stats](#reusable-executor-and-stats)
- [Options](#options)
- [How it works](#how-it-works)
- [Package layout](#package-layout)
- [API reference](#api-reference)
- [Benchmarks](#benchmarks)
- [References](#references)

## Install

```bash
go get github.com/lrweck/morsel
```

Then:

```go
import "github.com/lrweck/morsel"
```

## Getting started

```go
package main

import (
	"fmt"

	"github.com/lrweck/morsel"
)

type Debt struct{ Amount int }

func main() {
	debts := make([]*Debt, 1_000_000)
	for i := range debts {
		debts[i] = &Debt{Amount: i}
	}

	// ForEach: apply fn to every element in parallel.
	// No options needed — the defaults are sane (GOMAXPROCS workers, 256-item morsels).
	err := morsel.ForEach(debts, func(d *Debt) {
		d.Amount *= 2
	})
	if err != nil {
		panic(err)
	}

	// Pipeline: same sane defaults, no executor to manage.
	total, err := morsel.Slice(debts).
		Map(func(d *Debt) int { return d.Amount }).
		Reduce(0,
			func(acc, v int) int { return acc + v },
			func(a, b int) int { return a + b },
		)
	if err != nil {
		panic(err)
	}
	fmt.Println(total)
}
```

## When to use it — and when not to

**Good fit**

- **CPU-bound work over many elements** — transforms, parsing, scoring,
  aggregations, validation of large batches.
- **Irregular or mixed per-item cost.** Work stealing balances it: a worker that
  finishes its share early takes from a busy peer, which static chunking cannot.
- **Bounded memory while streaming.** Large files, network streams, or query
  results are processed in morsels without materializing everything.
- **You want cancellation and error propagation** without hand-rolling a pool.
- **You do not want to tune it.** Small inputs run on the caller and few-morsel
  runs use few workers, so the same call is right for one item and ten million
  (see [Benchmarks](#benchmarks)).

**Poor fit**

- **Trivial work per element over a large input.** The library calls your
  callback once per element, so when an element costs a few ns the call overhead
  dominates: a plain `for` loop is ~2.5–3× faster (1M `sum += v`: 321 µs vs
  880 µs at 16 workers). More workers narrow the gap but cannot close it. Small
  inputs are unaffected — they run on the caller.
- **Strict global ordering of results.** `Collect`, `MapSeq`, and iterator
  `Reduce` are unordered; only `MapSlice` preserves order.
- **A callback that must mutate shared state.** Independent work is what pays; a
  shared lock serializes the run.

**Fine, but maybe overkill**

- **A handful of concurrent I/O requests.** `errgroup` with `SetLimit` is a
  smaller tool, and just as valid. The library also does it — `MaxWorkers(n)`
  bounds the concurrency — and pulls ahead once there is CPU work or a stream
  (files, rows) to process.

## Compared to traditional Go

### 1. `ForEach` with error handling

**Traditional Go** — a worker pool fed by a channel. It is the most common
pattern, but you pay for it: a channel send **per element**, a fixed worker
count, manual error capture, and no balancing when per-item cost is irregular.

```go
func ForEachDebts(debts []*Debt) error {
	const workers = 8
	jobs := make(chan *Debt)
	errs := make(chan error, 1)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs {
				if err := calculate(d); err != nil {
					select {
					case errs <- err: // keep the first error
					default:
					}
					return
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, d := range debts {
			jobs <- d
		}
	}()

	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}
```

Problems: a hard-coded `8` (ignores the machine's cores), one channel
send/receive per element, errors from other workers can be lost, and with no
cancellation the rest keeps running after the first failure.

**With the library:**

```go
err := morsel.ForEachE(debts, func(d *Debt) error {
	return calculate(d)
})
```

- Workers scale up to `GOMAXPROCS` (and stop when there is no work).
- **One** queue operation per *morsel* (256 items), not per element.
- The first failure cancels the run and is returned.
- Irregular work is rebalanced by stealing.

### 2. Parallel map that preserves order

**Traditional Go** — static chunking. If `fn` is irregular (some items are much
more expensive), workers that finish early sit idle, because each one's share is
fixed.

```go
func MapParallel[T, R any](in []T, fn func(T) R) []R {
	out := make([]R, len(in))
	n := runtime.GOMAXPROCS(0)
	chunk := (len(in) + n - 1) / n

	var wg sync.WaitGroup
	for i := 0; i < len(in); i += chunk {
		end := min(i+chunk, len(in))
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for j := lo; j < hi; j++ {
				out[j] = fn(in[j]) // each goroutine writes its own region: no lock
			}
		}(i, end)
	}
	wg.Wait()
	return out
}
```

**With the library:**

```go
out, err := ex.MapSlice(ctx, in, fn)
```

The same order guarantee (each morsel writes only its own region, no `append`,
no lock), but with stealing to balance irregular loads, and propagated errors.

### 3. Reduce

**Traditional Go** — per-worker partials plus a manual merge:

```go
func SumParallel(in []int) int {
	n := runtime.GOMAXPROCS(0)
	partials := make([]int, n)
	chunk := (len(in) + n - 1) / n

	var wg sync.WaitGroup
	for i := range partials {
		lo := i * chunk
		hi := min(lo+chunk, len(in))
		wg.Add(1)
		go func(i, lo, hi int) {
			defer wg.Done()
			s := 0
			for _, v := range in[lo:hi] {
				s += v
			}
			partials[i] = s
		}(i, lo, hi)
	}
	wg.Wait()

	total := 0
	for _, p := range partials {
		total += p
	}
	return total
}
```

**With the library:**

```go
total, err := ex.ReduceSlice(ctx, in, 0,
	func(acc, v int) int { return acc + v }, // local fold per worker
	func(a, b int) int { return a + b },     // merge the partials
)
```

`init` must be an identity for both fold and merge (`0` for sums, `1` for
products, `nil` for concatenation), and the functions should be
associative/commutative — morsel order is not defined.

### 4. Errors + cancellation with `errgroup`

The idiomatic "traditional" approach is usually `golang.org/x/sync/errgroup` —
an **external dependency** that still leaves chunking to you:

```go
g, ctx := errgroup.WithContext(ctx)
n := runtime.GOMAXPROCS(0)
chunk := (len(in) + n - 1) / n
for i := 0; i < len(in); i += chunk {
	lo, hi := i, min(i+chunk, len(in))
	g.Go(func() error {
		for j := lo; j < hi; j++ {
			if err := process(ctx, in[j]); err != nil {
				return err
			}
		}
		return nil
	})
}
err := g.Wait()
```

**With the library:** zero dependencies, built-in cancellation, and balancing:

```go
ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()

err := morsel.ForEachE(in, func(v Item) error {
	return process(ctx, v)
}, morsel.WithContext(ctx))
```

### 5. Streaming (lines, chunks, rows)

**Traditional Go** — to process a large file in parallel you build a pipeline by
hand (reader → line channel → workers → result), taking care of backpressure so
you do not materialize the whole file.

```go
scanner := bufio.NewScanner(file)
lines := make(chan string, 1024)
go func() {
	defer close(lines)
	for scanner.Scan() {
		lines <- scanner.Text()
	}
}()

// ... N workers reading from `lines`, plus error/end synchronization ...
```

**With the library** — the producer is bounded (see
[Streaming and resource usage](#streaming-and-resource-usage)):

```go
err := morsel.Lines(file).
	MapE(parseLine).
	ForEach(func(rec Record) { store(rec) }, morsel.MorselSize(1024))
```

## Pipeline

Stages are chained with **generic methods** (Go 1.27), so every stage is typed at
compile time and nothing is boxed:

```go
got, err := morsel.Slice(debts).
	Map(validate).        // func(Debt) Debt
	Filter(isEligible).   // func(Debt) bool
	Map(calculate).       // func(Debt) Result
	Collect()
```

Terminals: `ForEach`, `ForEachE`, `Collect`, `Reduce`.

```go
total, err := morsel.Slice(debts).
	Map(func(d Debt) int { return d.Amount }).
	Reduce(0,
		func(acc, v int) int { return acc + v },
		func(a, b int) int { return a + b },
	)
```

Available stages: `Map`, `MapE` (fallible), `Filter`, `FlatMap`.

> Note: `Collect`, `MapSeq`, and iterator `Reduce` do **not** preserve order
> (they concatenate per-worker partials). `MapSlice` preserves it.

## Adapters (sources)

| Adapter | Produces |
|---|---|
| `Slice(data)` | `Pipeline[T, T]` over a slice (zero-copy) |
| `Range(start, end)` | `Pipeline[int, int]` from `start` to `end-1` |
| `Iter(seq)` | `Pipeline[T, T]` from an `iter.Seq[T]` |
| `IterErr(seq2)` | same, for `iter.Seq2[T, error]` |
| `Chunks(r, size)` | `Pipeline[[]byte, []byte]`, one chunk per `Read` |
| `ChunksPooled(r, size)` | same, but the chunk buffers are recycled |
| `Lines(r)` | `Pipeline[string, string]` |
| `Rows(next)` | `Pipeline[T, T]` from a pull iterator (`sql.Rows`, `csv`) |
| `From(src)` | `Pipeline[T, T]` from a custom `Source[T]` |

```go
// Range
morsel.Range(0, 1_000_000).ForEach(func(i int) { work(i) })

// Chunks of an io.Reader
morsel.Chunks(r, 64<<10).ForEach(func(b []byte) { hash.Write(b) })

// Same, recycling the chunk buffers: the callback must not keep b.
morsel.ChunksPooled(r, 64<<10).ForEach(func(b []byte) { hash.Write(b) })

// Rows (e.g. database/sql)
rows, _ := db.Query("select id, amount from debts")
defer rows.Close()
err := morsel.Rows(func() (Debt, bool, error) {
	if !rows.Next() {
		return Debt{}, false, rows.Err()
	}
	var d Debt
	if err := rows.Scan(&d.ID, &d.Amount); err != nil {
		return Debt{}, false, err
	}
	return d, true, nil
}).ForEach(func(d Debt) { calculate(&d) })
```

## Streaming and resource usage

The library is designed so that **memory never scales with the input size**.

- **Bounded producer.** For an iterator or a reader, a single producer
  materializes morsels of at most `MorselSize` items and publishes them into the
  bounded queues. When the queues are full the producer blocks — that is the
  backpressure. Peak memory is
  `O(MaxWorkers × QueueCapacity × sizeof(morsel) + MorselSize × sizeof(T))`,
  plus one overflow queue when every worker queue fills; it does not grow with
  the length of the stream.
- **Slice path is zero-copy.** `Slice`/`ForEachSlice` hand out `data[start:end]`
  sub-slices; no element is ever copied into an intermediate buffer.
- **Idle workers cost ~0 CPU.** A worker with no work parks on a `sync.Cond`;
  there is no busy-wait. It is woken by the producer that feeds it.
- **Workers are bounded and transient.** At most `MaxWorkers` worker goroutines
  exist at any time, plus one short-lived watcher during a run. They are created
  on demand and stop when the run ends — an `Executor` keeps none alive between
  runs.
- **Sequential fast path.** If the whole input fits in one morsel, or
  `MaxWorkers` is 1, the run happens in order on the calling goroutine — no
  workers, no queues, no extra goroutines. The trigger is the input size, not
  `MaxWorkers`: a small input stays sequential even with `MaxWorkers(8)`; lower
  `MorselSize` if you want it parallel instead.

Resource sketch at defaults (`MaxWorkers=GOMAXPROCS`, `MorselSize=256`,
`QueueCapacity=32`):

| Resource | Cost |
|---|---|
| Worker goroutines | ≤ `MaxWorkers` (+1 watcher while running) |
| Queue memory | `MaxWorkers × QueueCapacity × 48 B` ≈ 1.5 KB/worker, plus the overflow queue only when used |
| In-flight elements | ≤ `MorselSize × sizeof(T)` per queued morsel |
| Allocations per morsel (primitives) | **0** |
| CPU when idle | ~0 (parked) |
| CPU when busy | ≤ `MaxWorkers` cores |

## Errors and cancellation

Every operation takes a `context` (via `WithContext` or the `ctx` parameter of
the primitives). The first failure records the error, cancels the run, and is
returned; work not yet started is dropped, work already running finishes.

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

err := ex.ForEachSlice(ctx, data, func(v Item) error {
	return process(v)
})
switch {
case errors.Is(err, context.DeadlineExceeded):
	// timed out
case err != nil:
	// callback error (the first one observed)
}
```

Callback panics: by default they **propagate** (crash), as in normal Go. Enable
`RecoverPanics(true)` to turn them into an error:

```go
err := morsel.ForEach(data, func(v int) {
	panic("boom") // becomes an error instead of crashing
}, morsel.RecoverPanics(true))
```

## Reusable executor and stats

`Executor` holds **only configuration and stats** — each run creates its own
state, so running in sequence is safe and interference-free:

```go
ex := morsel.NewExecutor(morsel.Config{
	MaxWorkers:    8,
	MorselSize:    256,
	QueueCapacity: 32,
	StealAttempts: 4,
})

for _, req := range requests {
	if err := ex.ForEachSlice(ctx, req.Data, process); err != nil {
		return err
	}
}

s := ex.Stats()
fmt.Printf("morsels=%d steals=%d/%d workers=%d\n",
	s.MorselsExecuted, s.StealsSucceeded, s.StealsAttempted, s.WorkersCreated)
```

To reuse an executor from the ergonomic API: `morsel.WithExecutor(ex)`.

## Options

Every option is optional. With no options — or a zero-value `Config` — the
library uses `DefaultConfig()`:

| Field | Default | Meaning |
|---|---|---|
| `MaxWorkers` | `GOMAXPROCS` | worker ceiling (not a target) |
| `MorselSize` | `256` | items per morsel |
| `QueueCapacity` | `32` | capacity of each queue (bounded) |
| `StealAttempts` | `4` | victims probed before parking |
| `RecoverPanics` | `false` | turn a callback panic into an error |

The numeric fields are **unsigned**, so an invalid (negative) configuration is
impossible; `NewExecutor` cannot fail.

Pass only what you want to override, as trailing `Option` arguments:

```go
// Defaults are fine for most calls:
morsel.ForEach(data, func(v int) { work(v) })

// Override only what matters here:
morsel.ForEachE(data,
	func(v int) error { return work(v) },
	morsel.MaxWorkers(4),
	morsel.MorselSize(64),
)
```

The same applies to `NewExecutor`: `NewExecutor(morsel.Config{})` is valid and
equivalent to `NewExecutor(morsel.DefaultConfig())`.

The run options `WithContext(ctx)` and `WithExecutor(ex)` are also optional and
also come last.

## How it works

```
             producer (caller's goroutine)
                     │  direct push, round-robin
     ┌───────────────┼───────────────┐
     ▼               ▼               ▼
  W0 SPMC queue  W1 SPMC queue  W2 SPMC queue      ← bounded, lock-free
     │               │               │
   local pop      steal ◄──────────► steal
     │               │               │
     ▼               ▼               ▼
   execute        execute         execute
```

- **Morsel**: `Work[T]{Start, Items, Release}` — 40 bytes (`Release` stays nil
  for most sources); the slice path stores
  `data[start:end]` (a zero-copy sub-slice, no allocation).
- **Per-worker queue → SPMC** (single-producer/multi-consumer) with per-cell
  sequence numbers (Vyukov): a single producer, consumers are the owner plus
  thieves. Lock-free and clean under `-race`.
- **Injection → MPMC** (Vyukov) used only as *overflow* when every queue is
  full. The hot path never touches it, and it is allocated only if that happens.
- **Work stealing**: local queue → steal (pseudo-random victim, at most
  `StealAttempts`) → overflow → park.
- **Lifecycle**: workers spin up on demand; they stop when `producerDone &&
  pending == 0`. Parking/waking uses `sync.Cond` (~28ns vs ~120ns for a channel).
- **Explicit completion**: `producerDone + pending`, not just a `WaitGroup`.
- **Lock-free reduce**: each worker folds into its own accumulator; the partials
  are merged at the end.

## Package layout

The public package is a thin facade; the machinery lives in `internal/` and is
not importable from outside:

```
morsel/                 public API
  morsel.go             doc, errors, Options, ForEach/ForEachE/...
  config.go             Config, Stats, DefaultConfig
  executor.go           Executor
  foreach.go map.go reduce.go   primitives
  pipeline.go           fluent pipeline + adapters
  batch.go              Batch, Source
  internal/engine/      scheduler: queues, workers, stealing, lifecycle
  internal/queue/       bounded lock-free SPMC and MPMC queues
```

## API reference

**Primitives (Executor):**

```go
func NewExecutor(cfg Config) *Executor
func (ex *Executor) ForEachSlice[T any](ctx context.Context, data []T, fn func(T) error) error
func (ex *Executor) ForEachSeq[T any](ctx context.Context, seq iter.Seq[T], fn func(T) error) error
func (ex *Executor) ForEachSeqErr[T any](ctx context.Context, seq iter.Seq2[T, error], fn func(T) error) error
func (ex *Executor) MapSlice[T, R any](ctx context.Context, data []T, fn func(T) R) ([]R, error)
func (ex *Executor) MapSeq[T, R any](ctx context.Context, seq iter.Seq[T], fn func(T) R) ([]R, error)
func (ex *Executor) ReduceSlice[T, R any](ctx context.Context, data []T, init R, fold func(R, T) R, merge func(R, R) R) (R, error)
func (ex *Executor) ReduceSeq[T, R any](ctx context.Context, seq iter.Seq[T], init R, fold func(R, T) R, merge func(R, R) R) (R, error)
func (ex *Executor) Stats() Stats
```

**Package-level form of the slice primitives** (the shapes in the spec; the
`Executor` methods above are the same calls):

```go
func ForEachSlice[T any](ctx context.Context, ex *Executor, data []T, fn func(T) error) error
func MapSlice[T, R any](ctx context.Context, ex *Executor, data []T, fn func(T) R) ([]R, error)
func MapSeq[T, R any](ctx context.Context, ex *Executor, seq iter.Seq[T], fn func(T) R) ([]R, error)
func ReduceSlice[T, R any](ctx context.Context, ex *Executor, data []T, init R, fold func(R, T) R, merge func(R, R) R) (R, error)
func ReduceSeq[T, R any](ctx context.Context, ex *Executor, seq iter.Seq[T], init R, fold func(R, T) R, merge func(R, R) R) (R, error)
```

**Ergonomic (no explicit `Executor`):**

```go
func ForEach[T any](data []T, fn func(T), opts ...Option) error
func ForEachE[T any](data []T, fn func(T) error, opts ...Option) error
func ForEachSeq[T any](seq iter.Seq[T], fn func(T), opts ...Option) error
func ForEachSeqE[T any](seq iter.Seq[T], fn func(T) error, opts ...Option) error
func ForEachSeqErr[T any](seq iter.Seq2[T, error], fn func(T) error, opts ...Option) error
```

**Pipeline:**

```go
func Slice[T any](data []T) Pipeline[T, T]
func Range(start, end int) Pipeline[int, int]
func Iter[T any](seq iter.Seq[T]) Pipeline[T, T]
func IterErr[T any](seq iter.Seq2[T, error]) Pipeline[T, T]
func Chunks(r io.Reader, size int) Pipeline[[]byte, []byte]
func ChunksPooled(r io.Reader, size int) Pipeline[[]byte, []byte]
func Lines(r io.Reader) Pipeline[string, string]
func Rows[T any](next func() (T, bool, error)) Pipeline[T, T]
func From[T any](src Source[T]) Pipeline[T, T]

func (p Pipeline[Src, T]) Map[U any](fn func(T) U) Pipeline[Src, U]
func (p Pipeline[Src, T]) MapE[U any](fn func(T) (U, error)) Pipeline[Src, U]
func (p Pipeline[Src, T]) Filter(pred func(T) bool) Pipeline[Src, T]
func (p Pipeline[Src, T]) FlatMap[U any](fn func(T) []U) Pipeline[Src, U]
func (p Pipeline[Src, Out]) ForEach(fn func(Out), opts ...Option) error
func (p Pipeline[Src, Out]) ForEachE(fn func(Out) error, opts ...Option) error
func (p Pipeline[Src, Out]) Collect(opts ...Option) ([]Out, error)
func (p Pipeline[Src, Out]) Reduce[U any](init U, fold func(U, Out) U, merge func(U, U) U, opts ...Option) (U, error)
```

**Types:**

```go
// A materialized morsel. Iterators have no random access, so their elements
// arrive in Batches.
type Batch[T any] struct {
	Items   []T
	Release func() // optional; see ChunksPooled
}

// Source yields Batches to a single producer; returning an error aborts the
// run.
type Source[T any] func(yield func(Batch[T]) bool) error
```

Set `Batch.Release` when a source recycles element buffers: the engine calls it
once, after the batch's elements have been processed, so a custom source can pool
them the way `ChunksPooled` does. The elements are only valid until `Release`
runs.

`Config` is described in [Options](#options) and `Stats` in
[Reusable executor and stats](#reusable-executor-and-stats).

Requires **Go 1.27** (generic methods).

## Benchmarks

`GOMAXPROCS=20` (i7-13700H), `go test -bench -benchtime=3s -benchmem`. Benchmarks
are named by what they run: `BaselineLoop` is a plain `for` loop, `ChannelPool`
is a traditional goroutine pool fed by a channel, and `Morsel*` is this library.

Run it yourself:

```bash
go test -race ./...
go test -run='^$' -bench=. -benchmem ./...
```

### Heavy per-element work — the library's home turf

1M `int`s, 64 iterations of work per element:

| approach | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BaselineLoop` — plain `for` loop | 54.4 ms | 0 | 0 |
| `ChannelPool` — goroutine pool + channel | 15.2 ms | 1.5 KB | 21 |
| **`Morsel`** — this library | **7.2 ms** | 35 KB | 93 |

**~7.5× faster than the plain loop and ~2.1× faster than the channel pool.**

### Scaling with workers (`Morsel`, same heavy workload)

| `MaxWorkers` | ns/op | B/op | allocs/op |
|---:|---:|---:|---:|
| 1 (runs on the caller) | 57.3 ms | 200 | 4 |
| 2 | 32.3 ms | 5.4 KB | 19 |
| 4 | 17.6 ms | 8.7 KB | 27 |
| 8 | 11.6 ms | 15 KB | 43 |
| 16 | 7.9 ms | 28 KB | 75 |

One worker is within ~10% of the plain loop (`BaselineLoop` 54.4 ms) — the
sequential path adds almost nothing.

### Small inputs cost nothing

You never have to guess a "right" `MaxWorkers`. A tiny **input** never touches
the pool: if the whole input fits in one morsel — or `MaxWorkers` is 1 — the loop
runs on the calling goroutine, with no workers, queues or extra goroutines. The
trigger is the input size, not `MaxWorkers`, so a small input stays inline even
with `MaxWorkers(8)`.

| benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `TinyInput/sequential` — 100 elements, on the caller | **347 ns** | 352 | 6 |
| `TinyInput/pooled` — 100 elements, forced through the pool | 14.5 µs | 7.3 KB | 27 |

~**42×** cheaper and ~**21×** lighter on allocation when it stays on the caller.

On the pooled path, workers track the backlog: the pool grows to
`min(MaxWorkers, morsels queued)`, so a two-morsel run uses two workers, not
eight, and only the workers that start are allocated.

### Trivial work over a large input — the poor fit

The library still scales, but the per-element call overhead keeps it behind a
plain loop.

| benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Light/baseline` — 1M `sum += v`, plain loop | **321 µs** | 0 | 0 |
| `Light/1` — 1M trivial, one worker | 1.46 ms | 200 | 4 |
| `Light/16` — 1M trivial, 16 workers | 0.88 ms | 28 KB | 75 |

It scales ~1.7× from 1 to 16 workers but never catches the plain loop: a
callback-based engine cannot inline a few-ns body. Use it when the per-element
work is real.

The per-morsel cost is dominated by your own callback: the profile shows the
engine at ~5% and the callback at ~95%. `MorselSize` and worker sweeps are in the
suite; 256 is a reasonable default.

### Allocations

Allocation is **per run**, not per element: the slice primitives allocate
**nothing per morsel**. A morsel is a 40-byte value (`start` + a slice header plus
an optional release hook) pushed through the queues by value.

- The only per-run allocations are the queues, worker state and overflow buffer
  of the workers that actually spawn — all created lazily, so a run that needs
  two workers pays for two. The count tracks how many workers join, not how many
  elements there are: 1K and 100K elements differ only because the larger input
  starts more workers.
- The **pipeline** pays two small closures per morsel for its `emit` chain, but
  its intermediate element buffers are pooled per stage (`sync.Pool`), so there
  is no per-element allocation.

`go test -bench -benchmem` (`GOMAXPROCS=20`):

| call | B/op | allocs/op |
|---|---:|---:|
| `ForEachSlice`, 1K elements | 7.2 KB | 23 |
| `ForEachSlice`, 100K elements | 15 KB | 41 |
| `ForEachSlice`, 1M elements | 15 KB | 42 |
| Pipeline `Map` + `Reduce`, 1M | 448 KB | 8493 |

If you need the last drop of throughput on a hot path, prefer the primitives
(`ForEachSlice`/`MapSlice`/`ReduceSlice`) over the pipeline, and reuse an
`Executor` with `WithExecutor` so the configuration and stats stay put.

### Real I/O: `Lines` vs `Chunks` vs the stdlib iterator

`Lines` materializes one `string` per line, so line-oriented processing pays an
allocation **per line**. `Chunks` reads the file in blocks, and splitting lines
inside a block allocates nothing per line. `ChunksPooled` is `Chunks` with the
chunk buffers recycled. The engine also consumes a stdlib iterator directly,
since `Iter` takes any `iter.Seq[T]`.

#### At scale the disk is the bottleneck

One 5 GiB file, a single timed read per measurement, with the page cache dropped
between runs (`dd iflag=nocache`) so a cold disk read can be compared with a warm
one, `GOMAXPROCS=20`:

| approach | cold (disk) | warm (RAM) | gain | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| `dd` — raw read, the ceiling | 6.0 s · 893 MB/s | 0.52 s · 10.3 GB/s | 11.5× | — | — |
| sequential `bufio.Scanner` | 10.4 s · 516 MB/s | 8.6 s · 626 MB/s | 1.2× | 4.2 KB | 4 |
| `Lines(f)` | 17.0 s · 315 MB/s | 15.9 s · 339 MB/s | 1.1× | 11.8 GB | 279 M |
| `Chunks(f, 64 KiB)` | 6.3 s · 853 MB/s | 1.6 s · 3306 MB/s | 3.9× | 5.37 GB | 246 K |
| `Chunks(f, 256 KiB)` | 6.3 s · 850 MB/s | 1.5 s · 3550 MB/s | 4.2× | 5.37 GB | 62 K |
| **`ChunksPooled(f, 64 KiB)`** | **6.1 s · 875 MB/s** | **0.96 s · 5582 MB/s** | **6.4×** | 4–32 MB | 83 K |
| **`ChunksPooled(f, 256 KiB)`** | **6.1 s · 879 MB/s** | **0.96 s · 5604 MB/s** | **6.4×** | 4–42 MB | 21 K |

- **Cold, `Chunks` reads the disk and stops there**: ~850–880 MB/s against the
  ~890 MB/s `dd` ceiling. At scale the wall clock is the device, not the CPU.
- **Sequential and `Lines` barely move** (1.1–1.2×): the disk was never their
  limit — they are CPU- and GC-bound, and 5–8× slower than `Chunks`.
- **Warm, `Chunks` reaches 3.3–3.6 GB/s and `ChunksPooled` 5.6 GB/s** — parallel
  CPU over RAM. The 5 GiB that `Chunks` copies and reallocates is real work once
  the disk is out of the way.
- The small-file table below runs entirely from the page cache, so read it as an
  engine comparison, not as streaming throughput.

#### `Chunks` vs `ChunksPooled`: ownership, not throughput

`Chunks` gives every chunk a fresh, owned `[]byte`, so a 5 GiB file allocates
5.37 GB in total. `ChunksPooled` recycles buffers and hands the callback a slice
that is only valid until it returns, so its total allocation tracks the chunks in
flight (at most `MaxWorkers × QueueCapacity`) instead of the file. Both saturate
the disk when cold; the difference is the footprint, plus ~50% on light work when
the disk is *not* the limit (3950 vs 2640 MB/s on the 14 MB file).

#### Warm cache, 14 MB file

Same 1M-line (~14 MB) file the other benchmarks generate, `GOMAXPROCS=20`,
`go test -bench -benchtime=2s`:

| approach | light work (parse + 32 ops) | heavy work (parse + 2048 ops) |
|---|---:|---:|
| sequential `bufio.Scanner` | 33 ms · 440 MB/s · 4 allocs | 1.75 s · 8.2 MB/s · 4 allocs |
| stdlib line iterator (read-all, sequential) | 24 ms · 593 MB/s · 5 allocs | 1.79 s · 8.0 MB/s · 5 allocs |
| goroutine pool + channel | 168 ms · 85 MB/s · 1.0M allocs | 421 ms · 34 MB/s · 1.0M allocs |
| `Lines(f)` | 46 ms · 310 MB/s · 1.0M allocs | 163 ms · 88 MB/s · 1.0M allocs |
| `Iter` over the stdlib iterator | 39 ms · 364 MB/s · 7.9K allocs | 164 ms · 87 MB/s · 7.9K allocs |
| `Chunks(f, 64 KiB)` | 5.4 ms · 2.6 GB/s · 721 allocs | 167 ms · 86 MB/s · 758 allocs |
| **`ChunksPooled(f, 64 KiB)`** | **3.6 ms · 4.0 GB/s · 558 allocs** | 168 ms · 85 MB/s · 1.0K allocs |
| `Chunks(f, 256 KiB)` | 5.9 ms · 2.4 GB/s · 223 allocs | 191 ms · 75 MB/s · 266 allocs |
| `Chunks(f, 1 MiB)` | 8.5 ms · 1.7 GB/s · 84 allocs | 375 ms · 38 MB/s · 91 allocs |

- **`Chunks` wins both**: ~5–6× the sequential scan on light work and ~9× on
  heavy work. It avoids the per-line allocation *and* parallelizes.
- **`ChunksPooled` helps only when the callback is the cheap part**: +50% on
  light work, and nothing on heavy work — there the CPU is the limit, and the
  producer runs ahead until the in-flight window holds the whole file.
- **The stdlib iterator is fast sequentially** (24 ms, 5 allocs) but cannot
  parallelize on its own; fed to `Iter` it does (heavy: 1.79 s → 164 ms), at the
  cost of reading the whole file and materializing a slice header per line.
- **`Lines` is the convenient adapter**, but its per-line `string` caps it near
  sequential throughput when the work per line is small.
- **The traditional goroutine pool is the slowest parallel option**: it pays a
  channel send per line.
- Prefer **smaller chunks (64–256 KiB)**: more morsels means better balancing.
  1 MiB gives only ~14 morsels for this file, so the tail dominates.

## References

morsel is an independent implementation. It borrows ideas, not code — the
module still has no dependencies. The works behind the design:

- **[1] Morsel-Driven Parallelism** — Viktor Leis, Peter Boncz, Alfons Kemper,
  Thomas Neumann. SIGMOD 2014. <https://doi.org/10.1145/2588555.2610507>
  The core of it: split the input into morsels and let a dispatcher hand them to
  worker threads continuously, so the degree of parallelism is a runtime
  decision rather than a plan-time one. We take the morselization and the
  elastic dispatcher, not the NUMA placement.

- **[2] A bounded MPMC queue** — Dmitry Vyukov, 1024cores.net.
  <https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue>
  The per-cell sequence number scheme in `internal/queue`: the SPMC queues the
  workers pull from and the MPMC overflow. The rule that a cell's sequence number
  decides whose turn it is to touch it is his.

- **[3] Dynamic Circular Work-Stealing Deque** — David Chase, Yossi Lev. SPAA
  2005. <https://doi.org/10.1145/1073970.1073974>
  The reference work-stealing deque, and where the spec started. We ship [2]
  instead: Chase-Lev's benign stale read is still reported as a data race by
  `go test -race`, and a library should be clean under `-race` out of the box.
  What survives is the strategy — a thief takes from the far end and probes
  victims in a pseudo-random order.

- **[4] `iter`** — Go's push iterators (Go 1.23). <https://pkg.go.dev/iter>
  The shape of `Source`, `Batch`, `ForEachSeq` and the `Iter`/`IterErr`/`Rows`
  adapters: an iterator is a function you hand a `yield` callback, and stopping
  is `yield` returning false.

- **[5] `bufio.Scanner.Bytes`** — <https://pkg.go.dev/bufio#Scanner.Bytes>
  The borrowed-slice idiom behind `ChunksPooled`: hand out a view into a buffer
  that gets reused, and document that the view is only valid until the next call.

- **[6] Dave Cheney** — *Don't force allocations on the callers of your API*
  (2019) and `bytereader` (BSD-2-Clause).
  <https://dave.cheney.net/2019/09/05/dont-force-allocations-on-the-callers-of-your-api> ·
  <https://github.com/davecheney/bytereader>
  The article is the argument behind `Batch.Release`: the callee should not
  decide the allocation policy, so `Chunks` keeps returning owned chunks and
  `ChunksPooled` offers the borrowing variant rather than forcing either. We
  read `bytereader` while designing it — a sliding window over one reader with
  in-order release — and it does not fit a multi-consumer pipeline, so no code
  was taken from it.

## License

MIT — see [LICENSE](LICENSE).
