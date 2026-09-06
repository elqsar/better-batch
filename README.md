# better-batch

A Go library for high-throughput ingest with a durable buffer. Write single events in, get
batches out, survive a crash, and decide for yourself what happens when the sink can't keep
up.

```go
buf, err := batch.Open[Event](dir, sink, codec)
buf.Write(ctx, event)   // returns once the event is durable
```

It is a **library**, not a daemon and not a broker. The daemon slot is taken by the
OpenTelemetry Collector and Vector; this is the thing you embed in the process that is
already producing the events. If you need multiple processes or hosts to share one buffer,
use NATS or Kafka.

No dependencies outside the standard library.

## Install

```sh
go get github.com/elqsar/better-batch
```

```go
import batch "github.com/elqsar/better-batch"
```

## Quick start

```go
type Event struct {
    Name string
    N    int
}

sink := batch.SinkFunc[Event](func(ctx context.Context, b batch.Batch[Event]) error {
    return db.InsertAll(ctx, b.Records) // nil means "safely downstream, forget them"
})

buf, err := batch.Open[Event]("/var/lib/myapp/events", sink, batch.JSONCodec[Event]{},
    batch.WithFlush(1000, 4<<20, time.Second),          // batch at 1000 records, 4 MiB, or 1s
    batch.WithCapacity(1_000_000, 1<<30),               // cap the unflushed backlog
    batch.WithOnFull(batch.BlockThenDropOldest(5*time.Second)),
)
if err != nil {
    return err
}
defer buf.Close(ctx) // drains what is buffered before returning

for e := range events {
    if err := buf.Write(ctx, e); err != nil {
        return err
    }
}
```

## How it works

```
Write(ctx, T) ──► encode ──► [ WAL: segmented append-only log ] ──► returns
                                        │
                                   read cursor
                                        │
                                        ▼
                              [ batcher: count/bytes/time ]
                                        │
                                        ▼
                              Sink.Flush(ctx, Batch[T])
                                        │
                                   on success
                                        ▼
                            checkpoint (low-water mark) ──► segment deletion
```

Three goroutines: yours, one that group-commits appends to disk, one that reads the log,
assembles batches and drives the sink.

## Delivery semantics

**At-least-once, and only that.** `Write` returns once the record is committed to the log.
The checkpoint advances only after `Sink.Flush` returns nil. A crash between those two
points replays the batch.

There is no exactly-once mode, because it isn't achievable without the sink's cooperation.
What you get instead is a stable identity per **record**: `Batch.ID` is the log sequence
number of the batch's first record, and a batch always holds consecutive records, so
record `i` is `b.ID + i`. That number names one record for the life of the buffer — across
retries, across restarts, and across a crash that loses the log's tail. Give a sink with
idempotency keys or a transactional destination those numbers and you get
effectively-once:

```go
sink := batch.SinkFunc[Event](func(ctx context.Context, b batch.Batch[Event]) error {
    rows := make([]Row, len(b.Records))
    for i, e := range b.Records {
        // Same key on every retry and after a crash, so the destination can dedupe.
        rows[i] = Row{Key: b.ID + uint64(i), Event: e}
    }
    return db.InsertIdempotent(ctx, rows)
})
```

**Deduplicate by record, not by batch.** Batch boundaries are not stable: after a restart
the same records can be regrouped, so a batch that was delivered as `{ID: 1, 5 records}`
may come back as `{ID: 1, 10 records}`. A sink that skips the whole batch because it has
seen ID 1 would silently discard the five records it had not. Record keys do not have this
problem, which is why they are the unit to deduplicate on.

## Durability

The columns are about writes that have **returned**. A write still in flight can always be
lost by a crash, in every mode.

| Mode | `Write` returns when | Process crash | Machine crash |
| --- | --- | --- | --- |
| `SyncPeriodic` (default, 5ms) | the record is fsynced, after waiting up to the interval for company | nothing lost | nothing lost |
| `SyncAlways` | the record is fsynced, with no wait | nothing lost | nothing lost |
| `SyncNever` | the record is in the page cache | nothing lost | recent writes lost |

`SyncPeriodic` and `SyncAlways` differ in latency and fsync count, not in what survives:
the interval is how long a write waits so that concurrent writes can share one fsync. It is
not a loss window — `Write` does not return until its own record is durable.

```go
batch.WithSync(batch.SyncAlways, 0)
```

`SyncNever` is the fast mode and still survives process death, because the page cache
outlives the process. Only a machine crash loses anything.

**Throughput is records-per-commit-round divided by fsync latency.** The levers are more
concurrent writers, or `WriteBatch`:

```go
buf.WriteBatch(ctx, e1, e2, e3) // one commit round, one fsync, one capacity decision
```

A single goroutine calling `Write` in a loop cannot go faster than the device — there is
only ever one record pending, so there is nothing for group commit to amortise.

### When a write's context expires

A record is staged before it is committed, and staging is not reversible. If `ctx` expires
in between, the record still lands. `Write` says so with `ErrUncertain`:

```go
switch err := buf.Write(ctx, e); {
case err == nil:
case errors.Is(err, batch.ErrUncertain):
    // Staged, and probably delivered. Retrying is safe but produces a
    // duplicate, which at-least-once already allows.
case err != nil:
    // Not written.
}
```

The wrapped context error is preserved, so `errors.Is(err, context.DeadlineExceeded)` still
works. The buffer settles its own accounting in the background either way — the backlog
figures in `Stats()` stay exact whichever way the commit round goes.

## Backpressure

Full is a policy decision, not a constant. Every built-in is one line:

| Policy | Behaviour |
| --- | --- |
| `Block()` (default) | wait for the sink to catch up; propagates backpressure to the producer |
| `Reject()` | fail the write with `ErrFull` |
| `DropNewest()` | discard the incoming write, count it |
| `DropOldest()` | discard the head of the backlog to make room |
| `BlockThenDropOldest(d)` | block for `d`, then start shedding |

The same path handles a full **disk**, distinguished by `State.DiskFull`. Writes then fail
with `ErrDiskFull` under `Reject`, and the buffer stays usable: space comes back as the sink
drains and segments are truncated.

Custom policies see everything they need to shed intelligently:

```go
batch.WithOnFull(batch.PolicyFunc(func(ctx context.Context, s batch.State) batch.Decision {
    switch {
    case s.DiskFull:
        return batch.DecisionReject      // nothing local can fix this quickly
    case s.SinkFailures > 10:
        return batch.DecisionDropOldest  // the destination is down, keep fresh data
    case s.OldestAge > time.Minute:
        return batch.DecisionDropOldest  // this backlog is too stale to be worth sending
    default:
        return batch.DecisionBlock
    }
}))
```

`DropOldest` drops records still in the log one at a time, but a batch already handed to the
sink can only be abandoned whole, so under a failing sink it may discard more than strictly
necessary.

## Retries and dead letters

By default a failed flush retries forever with exponential backoff and jitter — a durable
buffer that drops data on a transient outage is not doing its job. To bound it:

```go
batch.WithRetry(batch.Backoff{Initial: 100 * time.Millisecond, Max: 30 * time.Second}, 10),
batch.WithDeadLetter[Event](deadLetterSink),
```

With a positive attempt limit and no dead-letter sink, exhausted batches are dropped and
counted (`ReasonRetriesExhausted`).

---

# Observability

Two mechanisms, split along a deliberate line:

- **`Stats()`** for things that *are* — backlog depth, pending bytes, checkpoint position,
  consecutive sink failures. Poll it for gauges.
- **`Observer`** for things that *happen* — writes, flush attempts, drops, backpressure
  decisions, checkpoints. Register callbacks for counters and histograms.

The library depends on no metrics package. You wire it to yours.

## The Observer

Every field is optional; a nil field is skipped, so an Observer that fills in one hook costs
nothing for the rest.

```go
type Observer struct {
    OnWrite        func(records, bytes int)
    OnFlush        func(FlushInfo)
    OnDrop         func(records int, reason DropReason)
    OnDeadLetter   func(records int)
    OnBackpressure func(State, Decision)
    OnCheckpoint   func(lsn uint64)
}
```

| Hook | Fires | Good for |
| --- | --- | --- |
| `OnWrite` | once per accepted `Write`/`WriteBatch` | ingest rate, bytes/sec |
| `OnFlush` | after **every** sink attempt, success or failure | flush latency histogram, error rate, retry rate |
| `OnDrop` | records discarded and never delivered | data-loss alerting, split by `DropReason` |
| `OnDeadLetter` | a batch the dead-letter sink accepted | dead-letter rate |
| `OnBackpressure` | each time the policy is consulted | how often you are saturated, and what you did |
| `OnCheckpoint` | the low-water mark was persisted | replay window, progress |

`FlushInfo` carries `ID`, `Records`, `Bytes`, `Attempt`, `Duration` and `Err`. Because it
fires on failures too, one hook gives you both the latency histogram and the error counter.

`DropReason` is `ReasonPolicy`, `ReasonDecode`, or `ReasonRetriesExhausted`, and both it and
`Decision` implement `String()` so they drop straight into a metric label.

> **Callbacks run on the buffer's own goroutines.** Blocking in one blocks the pipeline, and
> calling back into the `Buffer` from one will deadlock. Keep them to incrementing a counter
> or observing a histogram.

## Prometheus

Complete and copy-pasteable, with `prometheus/client_golang`:

```go
package myapp

import (
    "context"
    "time"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"

    batch "github.com/elqsar/better-batch"
)

var (
    recordsWritten = promauto.NewCounter(prometheus.CounterOpts{
        Name: "events_buffer_records_written_total",
        Help: "Records accepted by the buffer.",
    })
    bytesWritten = promauto.NewCounter(prometheus.CounterOpts{
        Name: "events_buffer_bytes_written_total",
        Help: "Encoded bytes accepted by the buffer.",
    })
    flushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
        Name:    "events_buffer_flush_duration_seconds",
        Help:    "Duration of one sink flush attempt.",
        Buckets: prometheus.DefBuckets,
    }, []string{"outcome"})
    flushRetries = promauto.NewCounter(prometheus.CounterOpts{
        Name: "events_buffer_flush_retries_total",
        Help: "Flush attempts after the first for the same batch.",
    })
    recordsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "events_buffer_records_dropped_total",
        Help: "Records discarded and never delivered.",
    }, []string{"reason"})
    recordsDeadLettered = promauto.NewCounter(prometheus.CounterOpts{
        Name: "events_buffer_records_dead_lettered_total",
    })
    backpressure = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "events_buffer_backpressure_total",
        Help: "Backpressure policy invocations by decision.",
    }, []string{"decision", "disk_full"})

    // Gauges, filled in from Stats below.
    pendingRecords = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "events_buffer_pending_records",
    })
    pendingBytes = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "events_buffer_pending_bytes",
    })
    oldestAge = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "events_buffer_oldest_record_age_seconds",
    })
    sinkFailures = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "events_buffer_consecutive_sink_failures",
    })
    checkpointLSN = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "events_buffer_checkpoint_lsn",
    })
)

func observer() batch.Observer {
    return batch.Observer{
        OnWrite: func(records, bytes int) {
            recordsWritten.Add(float64(records))
            bytesWritten.Add(float64(bytes))
        },
        OnFlush: func(f batch.FlushInfo) {
            outcome := "success"
            if f.Err != nil {
                outcome = "error"
            }
            flushDuration.WithLabelValues(outcome).Observe(f.Duration.Seconds())
            if f.Attempt > 1 {
                flushRetries.Inc()
            }
        },
        OnDrop: func(records int, reason batch.DropReason) {
            recordsDropped.WithLabelValues(reason.String()).Add(float64(records))
        },
        OnDeadLetter: func(records int) {
            recordsDeadLettered.Add(float64(records))
        },
        OnBackpressure: func(s batch.State, d batch.Decision) {
            diskFull := "false"
            if s.DiskFull {
                diskFull = "true"
            }
            backpressure.WithLabelValues(d.String(), diskFull).Inc()
        },
        OnCheckpoint: func(lsn uint64) {
            checkpointLSN.Set(float64(lsn))
        },
    }
}

// pollStats fills the gauges. Stop it by cancelling ctx.
func pollStats(ctx context.Context, buf *batch.Buffer[Event], every time.Duration) {
    t := time.NewTicker(every)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-t.C:
            s := buf.Stats()
            pendingRecords.Set(float64(s.PendingRecords))
            pendingBytes.Set(float64(s.PendingBytes))
            oldestAge.Set(s.OldestAge.Seconds())
            sinkFailures.Set(float64(s.SinkFailures))
        }
    }
}
```

Wiring it up:

```go
buf, err := batch.Open[Event](dir, sink, codec, batch.WithObserver(observer()))
if err != nil {
    return err
}
go pollStats(ctx, buf, 10*time.Second)
```

## OpenTelemetry

The counters and histogram work the same way. Gauges are nicer here, because an observable
gauge can read `Stats()` on the SDK's own collection schedule — no polling goroutine:

```go
package myapp

import (
    "context"

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/metric"

    batch "github.com/elqsar/better-batch"
)

func instrument(ctx context.Context, buf *batch.Buffer[Event]) (batch.Observer, error) {
    m := otel.Meter("github.com/elqsar/better-batch")

    written, err := m.Int64Counter("batch.records.written",
        metric.WithDescription("Records accepted by the buffer."))
    if err != nil {
        return batch.Observer{}, err
    }
    flushDur, err := m.Float64Histogram("batch.flush.duration",
        metric.WithUnit("s"),
        metric.WithDescription("Duration of one sink flush attempt."))
    if err != nil {
        return batch.Observer{}, err
    }
    dropped, err := m.Int64Counter("batch.records.dropped")
    if err != nil {
        return batch.Observer{}, err
    }
    pressure, err := m.Int64Counter("batch.backpressure")
    if err != nil {
        return batch.Observer{}, err
    }

    // Gauges pull from Stats whenever the SDK collects.
    pendingRecords, err := m.Int64ObservableGauge("batch.pending.records")
    if err != nil {
        return batch.Observer{}, err
    }
    pendingBytes, err := m.Int64ObservableGauge("batch.pending.bytes")
    if err != nil {
        return batch.Observer{}, err
    }
    oldestAge, err := m.Float64ObservableGauge("batch.oldest.age", metric.WithUnit("s"))
    if err != nil {
        return batch.Observer{}, err
    }
    if _, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
        s := buf.Stats()
        o.ObserveInt64(pendingRecords, s.PendingRecords)
        o.ObserveInt64(pendingBytes, s.PendingBytes)
        o.ObserveFloat64(oldestAge, s.OldestAge.Seconds())
        return nil
    }, pendingRecords, pendingBytes, oldestAge); err != nil {
        return batch.Observer{}, err
    }

    return batch.Observer{
        OnWrite: func(records, bytes int) {
            written.Add(ctx, int64(records))
        },
        OnFlush: func(f batch.FlushInfo) {
            flushDur.Record(ctx, f.Duration.Seconds(), metric.WithAttributes(
                attribute.Bool("error", f.Err != nil),
                attribute.Int("attempt", f.Attempt),
            ))
        },
        OnDrop: func(records int, reason batch.DropReason) {
            dropped.Add(ctx, int64(records),
                metric.WithAttributes(attribute.String("reason", reason.String())))
        },
        OnBackpressure: func(s batch.State, d batch.Decision) {
            pressure.Add(ctx, 1, metric.WithAttributes(
                attribute.String("decision", d.String()),
                attribute.Bool("disk_full", s.DiskFull),
            ))
        },
    }, nil
}
```

Note the ordering: `RegisterCallback` needs the `*Buffer`, and `WithObserver` needs the
`Observer`. Open the buffer with the counters-only observer first and register the gauge
callback after, or keep a pointer the callback reads lazily.

## Minimal version

If you only want to know that data is being lost:

```go
batch.WithObserver(batch.Observer{
    OnDrop: func(records int, reason batch.DropReason) {
        slog.Warn("buffer dropped records", "count", records, "reason", reason)
    },
})
```

## What to alert on

| Signal | Why |
| --- | --- |
| `OnDrop` rate > 0 | you are losing data, whatever the reason says |
| `Stats().OldestAge` climbing | the sink cannot keep up; the backlog is going stale |
| `Stats().SinkFailures` > 0 for minutes | the destination is down, not just slow |
| `OnBackpressure` with `disk_full=true` | the filesystem is the constraint now |
| `Stats().PendingBytes` near `WithCapacity` | the next burst starts shedding or blocking |
| `Stats().CheckpointErr` non-nil | checkpointing keeps failing, usually a full disk |
| `Stats().Err` non-nil | unrecoverable; records stay durable and replay on the next `Open` |

`Stats().Err` and `CheckpointErr` are different in kind: `CheckpointErr` clears itself once a
checkpoint succeeds and the buffer keeps working meanwhile, while `Err` is terminal for that
`Buffer` instance.

---

## Configuration

| Option | Default | Notes |
| --- | --- | --- |
| `WithSync(mode, interval)` | `SyncPeriodic`, 5ms | durability vs. fsync cost |
| `WithFlush(records, bytes, interval)` | 1000, 4 MiB, 1s | batch-out triggers |
| `WithCapacity(records, bytes)` | 1e6, 1 GiB | cap on records the log still retains; 0 means unlimited |
| `WithOnFull(policy)` | `Block()` | what happens at capacity or on a full disk |
| `WithMaxInFlight(n)` | 1 | above 1 breaks ordering and the sink must be concurrent-safe |
| `WithRetry(backoff, maxAttempts)` | 100ms→30s, forever | 0 attempts means retry forever |
| `WithDeadLetter[T](sink)` | none | where exhausted batches go |
| `WithCheckpointInterval(d)` | 200ms | longer means fewer fsyncs, more replay after a crash |
| `WithSegmentBytes(n)` | 64 MiB | soft cap; segments may overshoot by one commit batch |
| `WithMaxRecordBytes(n)` | 4 MiB | largest single encoded record; lowering it below stored records fails `Open` |
| `WithObserver(o)` | none | metrics hooks |

## Codecs

`BytesCodec` and `StringCodec` store records verbatim; `JSONCodec[T]` is convenient rather
than fast. For a hot path, write your own:

```go
type Codec[T any] interface {
    Encode(dst []byte, v T) ([]byte, error) // append to dst, return the extended slice
    Decode(src []byte) (T, error)
}
```

`Encode` appends so it can avoid allocating per record. **`Decode` receives a slice that
aliases an internal read buffer and is only valid until the next record is read** — copy
anything you keep. A record that fails to decode is dropped with `ReasonDecode` rather than
wedging the pipeline behind it.

## Operational notes

- **One writer per directory**, enforced with a lock file. A second `Open` fails. The lock
  is `flock`, so enforcement is Unix-only; elsewhere the single-writer rule is yours to keep.
- **Sequence numbers are never reused.** They are claimed durably, a block at a time, before
  any record is handed one, so a crash that loses the log's tail resumes above the numbers
  that went with it rather than renumbering over them. The visible effect is a one-off gap
  in `Batch.ID` after an unclean restart — the price of a key the sink can trust. A clean
  `Close` gives the unused remainder of the block back, so an orderly restart continues
  where it left off.
- **Recovery** scans and checksum-verifies every segment on open. A bad record that
  reaches the end of the log is a torn tail — expected after a crash — and is truncated.
  A bad record with intact records after it is interior corruption and fails `Open`,
  because truncating there would silently discard durable records and the implicit LSN
  numbering cannot be reconstructed across a hole. A length field above the configured
  `WithMaxRecordBytes` is checksummed before anything is decided: if it verifies, the
  record is real and was written under a larger limit, and `Open` fails rather than
  deleting it. The one blind spot is a length field that is corrupt *and* fails its
  checksum in the final record's header: the next record boundary is unrecoverable, so it
  is indistinguishable from a torn header and is truncated.
- **Crash replay** starts from the last checkpoint, so `WithCheckpointInterval` sets how
  much gets re-delivered. Duplicates are the contract, not a bug. The checkpoint is fsynced
  while `SyncNever` records are not, so it can survive a machine crash that the log tail did
  not; `Open` clamps it back to the log's durable end. Nothing is lost — everything at or
  below the mark was already acked by the sink.
- **`Close(ctx)`** drains, checkpoints, and releases the directory. If `ctx` expires it stops
  waiting and returns the error; nothing is lost, since unacknowledged records replay on the
  next `Open`. A delivery that fails because the shutdown cancelled it is not counted as a
  retry that ran out, so it is neither dead-lettered nor dropped. A sink that ignores its
  context can still stall shutdown.
- **Sizing:** `WithCapacity` bounds the backlog on disk, so it is the number that decides how
  long an outage you can ride out. At 1 KiB per event, 1 GiB is roughly a million events. It
  counts every record the log still holds, including ones the sink has already taken: with
  `WithMaxInFlight` above 1 a batch that never completes pins the low-water mark, and the
  records acknowledged behind it stay on disk and keep holding their capacity until it lands.
  Records dropped by policy or a decode failure give theirs back as the flusher reads past
  them.

## Design notes

`DESIGN.md` covers the decisions, the storage format, the measurements, and the bugs found
along the way.
