# morsel examples

Runnable programs, each a `main` package:

```bash
go run ./example/foreach      # ForEach over a slice
go run ./example/pipeline     # Slice → Filter → Map → Reduce
go run ./example/errors       # error propagation + context cancellation
go run ./example/executor     # reusable Executor + Stats
go run ./example/iterator     # feed a stdlib iter.Seq into the engine
go run ./example/stream FILE  # read a file in chunks, bounded memory
```

| example | shows |
|---|---|
| `foreach` | the simplest entry point |
| `pipeline` | the fluent, typed API |
| `errors` | the first callback error aborts; `context` cancels the run |
| `executor` | reuse across runs and read `Stats` |
| `iterator` | `Iter` over any `iter.Seq[T]` (here `strings.Lines`) |
| `stream` | real disk I/O with memory bounded by the chunk size |
