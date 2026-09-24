# better-batch / clickhouse

A [better-batch](../README.md) sink that inserts each batch into a ClickHouse table through
[clickhouse-go/v2](https://github.com/ClickHouse/clickhouse-go).

It is a separate module, so the driver and its dependencies reach only programs that import
it. The core module stays dependency-free.

## Install

```sh
go get github.com/elqsar/better-batch/clickhouse
```

The package is called `clickhouse`, as is the driver's, so import one of them under another
name:

```go
import (
    batch "github.com/elqsar/better-batch"
    "github.com/elqsar/better-batch/clickhouse"

    ch "github.com/ClickHouse/clickhouse-go/v2"
)
```

## Usage

```go
conn, err := ch.Open(&ch.Options{Addr: []string{"localhost:9000"}})
if err != nil {
    return err
}

sink, err := clickhouse.New(conn, "analytics.events", []string{"ts", "user_id", "name"},
    func(e Event) []any { return []any{e.At, e.UserID, e.Name} },
    clickhouse.WithDeduplication("events-ingest-1"),
)
if err != nil {
    return err
}

buf, err := batch.Open[Event]("/var/lib/myapp/events", sink, batch.JSONCodec[Event]{},
    batch.WithFlush(50_000, 16<<20, time.Second),
    batch.WithOnSinkError(clickhouse.Classify),
)
```

One `Flush` is one `INSERT`. ClickHouse likes large, infrequent inserts, so batch by tens of
thousands of records or several megabytes, not by hundreds.

## What happens when an insert fails

| Failure | Returned error | With `Classify` |
|---|---|---|
| A value the driver cannot convert to its column type | wraps `batch.ErrPermanent` | dead-lettered |
| The server rejects the data (type mismatch, parse error, constraint) | wraps `batch.ErrPermanent` | dead-lettered |
| Bad credentials, unknown database, table or column | the driver's error | buffer stops, backlog kept |
| The row function returns the wrong number of values | a shape error | buffer stops, backlog kept |
| Anything else: network, timeouts, too many parts | the driver's error | retried on the ladder |

Without `Classify` the permanent errors are still dead-lettered, because the buffer reads
`ErrPermanent` itself. What `Classify` adds is `FailBuffer` for a destination that is wrong:
without it those batches are retried forever, which loses nothing but never gets anywhere.

One bad record fails its whole batch. Pair the buffer with `batch.WithDeadLetter` so the
batch goes somewhere you can inspect.

## Deduplication

The buffer delivers at least once, so a batch whose insert succeeded but whose
acknowledgement was lost is sent again. `WithDeduplication(scope)` sets
`insert_deduplication_token` to the scope plus the batch's range of record numbers, and the
server drops the repeat.

- The table must deduplicate inserts: `Replicated*MergeTree` does by default; a plain
  `MergeTree` needs `SETTINGS non_replicated_deduplication_window = N`.
- The scope must be unique among the buffers writing to the table, and must change if a
  buffer's directory is deleted and recreated. Record numbers are unique only within one
  directory; a reused token makes the server discard records it has never seen.
- A batch regrouped after a restart covers a different range and is inserted again.
  Deduplication narrows the window for duplicates, it does not close it. For exact
  deduplication, store `b.ID + i` in a column and use a `ReplacingMergeTree` keyed on it.

## Development

The integration tests need a server. From the repository root:

```sh
task ch:up      # ClickHouse in podman
task ch:test    # unit + integration tests
task ch:down
```

`CLICKHOUSE_ADDR`, `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` point the tests at another
server.

Releases are tagged `clickhouse/vX.Y.Z`, independently of the core's `vX.Y.Z`.
