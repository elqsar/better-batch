# better-batch — design

A Go **library** for high-throughput ingest with a durable buffer: write single events
in, get batches out, survive a crash, and control what happens when the sink can't keep up.

Not a daemon. Not a broker. If you need multi-process or multi-host, use NATS or Kafka —
this is the thing you embed in the process that is already producing the events.

> This is the design record: what was decided, why, and what the measurements said. It is
> kept as written rather than edited to match the result, so the sketches below show the
> intended shape, not the shipped API — some of it changed under contact with the problem.
> The README documents what actually exists.

## Goals

- **Durable ingest.** An event accepted by `Write` survives `kill -9`, within a documented
  and configurable loss window.
- **Batch out.** Events are handed to the sink in batches shaped by count, bytes, or time.
- **Real backpressure.** When the buffer is full the caller chooses the behaviour. Never a
  silent drop unless the caller explicitly asked for one.
- **Bounded resources.** Memory and disk are both capped. Running out of either is a
  backpressure event, not a crash.

## Non-goals

- Exactly-once delivery. Not achievable without sink cooperation; see *Semantics*.
- Multi-process or distributed access to one buffer directory. Single writer, enforced by
  a lock file.
- Stream processing — no windows, joins, or aggregation state. This is a buffer.
- Ordering across concurrent flushes (see `MaxInFlight`).

## Decisions

### 1. Library, not daemon

The daemon slot is taken by the OpenTelemetry Collector and Vector. The embeddable slot is
empty: today everyone either pulls in the whole Collector, stands up a broker, or hand-rolls
the tenth in-memory batcher. This targets the third group.

### 2. Durability is the point

"State management" here means crash resume, so the WAL is on the write path by default.
An event that only ever lived in memory is gone after a crash, so `Durable` mode appends
every record to the log *before* `Write` returns.

| Mode | Write path | Crash behaviour |
| --- | --- | --- |
| `Durable` (default) | WAL append → group commit → ack | Resume from last checkpoint |
| `Ephemeral` | memory, spill to disk only under pressure | In-flight events lost |

`Durable` is cheaper than it sounds: appends are sequential and group commit amortises the
fsync across all concurrent writers. The fsync is the cost, not the append.

### 3. At-least-once, sink-acked

Three separate things people conflate:

**Ack boundary — fixed, not configurable.** `Write` returns once the record is committed to
the WAL. The read checkpoint advances only after `Sink.Flush` returns nil. A crash between
those two points replays the batch. That is the contract, and it is at-least-once.

No exactly-once. Instead, every batch carries a stable monotonic `Batch.ID` (the LSN of its
first record) so a sink with idempotency keys or a transactional destination can dedupe on
its own side and get effectively-once. Honest, and actually usable.

**Durability — configurable.** `SyncAlways` / `SyncInterval(d)` / `SyncNever`. Default is
`SyncInterval(5ms)`: a bounded, documented loss window instead of a vague one.

> As built, this one came out stronger than planned. `Write` waits for its own record to be
> durable, so the interval turned into latency a write may wait, not data it may lose —
> `SyncPeriodic` loses no acknowledged write. See the README's durability table.

**Retry — configurable.** Backoff with max attempts, then the batch goes to a dead-letter
sink so one poison batch cannot wedge the pipeline.

### 4. Backpressure is a policy, not a constant

Every existing library hardcodes one behaviour, usually "drop newest", usually silently.
Here it is an interface:

```go
type Policy interface {
    OnFull(ctx context.Context, s State) Decision
}
```

with `Block`, `Reject`, `DropNewest`, `DropOldest` provided, and `State` carrying queue
depth, bytes, oldest-record age and consecutive sink failures so a custom policy can shed
by priority or age.

Both capacity limits feed the same path: memory/record caps *and* disk full. Disk full is
the one everyone forgets, and it is the most important one to get right.

### 7. Undeliverable records are the caller's decision, not the library's

Two places quietly threw records away, and both were reached exactly when the caller most
wanted durability.

**The dead-letter sink got one attempt.** The primary sink retries forever by default; the
sink of last resort had a single shot, and any error — a network blip — discarded the whole
batch as `ReasonRetriesExhausted`. That asymmetry is not defensible: a caller who configured
`WithDeadLetter` said "do not lose these". It now retries on the same ladder, but **bounded
by default** (three attempts), because the reason the original code gave up immediately is
still true — a dead-letter sink that is also down must not wedge the pipeline behind it.
`WithDeadLetterRetry` moves the bound, and 0 means forever.

**A record that would not decode was dropped.** Payloads are bytes the buffer itself wrote
and every record is checksummed, so a decode failure is not corruption — it is the codec
having changed between runs, a rollout replaying a backlog the previous build produced.
Dropping keeps the pipeline moving, which is the right default; but for a buffer whose whole
purpose is not losing records, "your deploy silently ate the backlog" is the wrong only
option. `WithOnDecodeFailure` hands over the LSN and the raw bytes, and takes back one of
two answers: `DropRecord`, or `StopBuffer`.

`StopBuffer` works by *not* acknowledging: the poisoned LSN never reaches `complete`, so the
low-water mark stays below it, `persist` cannot truncate it away, and the next `Open` reads
it again. Records already read ahead of it are dispatched first, so stopping costs no
duplicates it does not have to.

**Quarantine is a callback, not a directory.** Writing bad payloads to `dir/quarantine/`
would have meant a second on-disk format with its own rotation, cleanup, crash semantics and
disk-full path — and a new failure mode for when quarantining is what fails. Handing the
caller the bytes collapses *quarantine*, *stop* and *drop* into one option and keeps decision
6's shape: hooks, not implementations.

The one thing `StopBuffer` forced open: `b.fail` set the error and the flusher exited, but
nothing told the writers. A failed buffer never drains, so under the default `Block` policy
every writer parked forever with only `Stats().Err` to explain it. `Write` now returns
`ErrFailed` wrapping the cause. That was a pre-existing hole on the `read log` failure path
too; `StopBuffer` only made it reachable on purpose.

## Architecture

```
Write(ctx, T) ──► encode ──► [ WAL: segmented append-only log ] ──► ack
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

Three goroutines: the caller's (append), the committer (group commit + fsync), the flusher
(read → batch → sink → checkpoint).

## API sketch

*Written before implementation; `WithMode` and `SyncInterval(d)` did not survive it. See
the README for the API as shipped.*

```go
type Sink[T any] interface {
    Flush(ctx context.Context, batch Batch[T]) error
}

type Codec[T any] interface {
    Encode(dst []byte, v T) ([]byte, error)
    Decode(src []byte) (T, error)
}

func Open[T any](dir string, sink Sink[T], codec Codec[T], opts ...Option) (*Buffer[T], error)

func (b *Buffer[T]) Write(ctx context.Context, v T) error
func (b *Buffer[T]) WriteBatch(ctx context.Context, vs []T) error
func (b *Buffer[T]) Close(ctx context.Context) error   // drain, flush, checkpoint
```

```go
WithMode(Durable | Ephemeral)
WithSync(SyncAlways | SyncInterval(5*time.Millisecond) | SyncNever)
WithOnFull(Block | Reject | DropNewest | DropOldest | policy)
WithCapacity(maxBytes, maxRecords)             // triggers OnFull
WithFlush(maxRecords, maxBytes, maxInterval)   // triggers batch-out
WithMaxInFlight(n)                             // default 1; >1 breaks ordering
WithRetry(backoff, maxAttempts)
WithDeadLetter(sink)
```

## Storage

A **segmented append-only log**, not a B-tree or LSM. A FIFO queue already has its ordering;
paying B-tree write amplification for it (as the OTel Collector does with bbolt) is waste.

```
dir/
  LOCK                     flock, single writer
  00000000000000000001.log segment, named by base LSN
  00000000000000065537.log
  CHECKPOINT               last LSN successfully flushed to the sink
```

Record framing, little-endian:

```
+-----------+-----------+---------------------+
| len  u32  | crc32c u32| payload (len bytes) |
+-----------+-----------+---------------------+
```

- CRC covers the length field *and* the payload, so a corrupt length is caught.
- LSNs are implicit: `segment.baseLSN + index`. Saves 8 bytes per record, and on open the
  segment chain is verified (`seg[i+1].baseLSN == seg[i].baseLSN + seg[i].count`) so a
  missing or truncated middle segment is detected rather than silently shifting LSNs.
- `MaxSegmentBytes` is a **soft** limit: rotation happens between commit rounds, never
  mid-record, so a segment may overshoot by up to one commit batch.

Readers pull a 256 KiB window per syscall rather than reading each record where it lies.
Records are small and consumed strictly in order, so one read amortises across hundreds of
them; bytes below a segment's durable size never change, so a cached window needs no
invalidation.

### Recovery

On open, every segment is scanned and CRC-verified. The last segment is truncated at the
first bad or partial record — a torn tail is expected after a crash, not corruption. Scan
cost is sequential I/O, so recovery time is proportional to the *unflushed* backlog, which
retention keeps small.

## The hard parts

1. **Group commit.** Make-or-break for throughput. One committer goroutine; writers park on
   a commit-generation channel and wake when their LSN is durable. Context-aware, so a
   cancelled `Write` does not leak a goroutine.
2. **Checkpoint as a low-water mark.** With `MaxInFlight > 1` and retries, batch 3 can
   succeed before batch 2. Storing last-completed would silently eat batch 2 on a crash.
   The checkpoint is always the lowest un-acked LSN minus one.
3. **Torn tail.** The last record before a crash is usually partial. Per-record CRC,
   truncate at first bad record, continue.
4. **Disk full.** The real backpressure event, and the one that forced the log's commit
   handling to be rewritten. A failed commit used to poison the log for good, on the
   reasoning that a partial write leaves a fragment later appends would bury. It is
   recoverable instead: the segment is cut back to its last durable byte, the discarded
   records' LSNs are handed out again, and their appenders learn about it from a handle to
   the commit round they staged into. The handle, rather than the LSN, is what carries the
   outcome — the numbers come back, so the next round gives them to different records.
   Only a rollback that *itself* fails is terminal. `OnFull` then fires through the
   same path as the memory cap, with `State.DiskFull` set.

   The way back out is narrow and worth stating: only truncating a segment frees space, only
   a durable checkpoint permits truncation, and the checkpoint is itself a write to the full
   disk. So checkpoint failures are retried rather than fatal, and truncation signals the
   capacity gate directly. The order cannot be swapped to free space sooner without losing
   records on a crash.
5. **The checkpoint and the log have different durability.** The checkpoint is fsynced on
   every write; under `SyncNever` the records are not. A machine crash can therefore leave
   the mark pointing *past* the end of the recovered log — the two files disagree about how
   much history exists. Nothing is lost when they do, because a mark of N means every LSN at
   or below N was acked by the sink before it was recorded. But the mark is also where the
   flusher starts, and left alone it starts above every LSN the reopened log is about to hand
   out: new writes are never read, never delivered, and never release their capacity, so the
   buffer wedges at the cap while `Flush` reports success. `Open` clamps the mark to the
   log's durable tail. The general lesson is that any two files with different sync
   disciplines need their skew reconciled on open, in the direction the weaker one allows.
6. **Staging is not reversible, so a write's context cannot un-write it.** Once a record is
   staged it will be committed, whatever the caller does next. A `Write` whose context
   expires mid-round therefore has three possible fates, not two, and the buffer must
   account for all of them: committed (keep the reservation, the flusher will release it on
   delivery), rolled back (release it now), or still undecided. Treating the third as failure
   — releasing the reservation and returning an error — double-releases the moment the record
   commits, driving the backlog counters negative and letting `reserve` over-admit against a
   cap it can no longer measure. The writer hands the outcome to a settler goroutine that
   waits out the round, and `ErrUncertain` tells the caller the truth in the meantime.
   Cancelling a write cancels the *waiting*, never the write.

## Testing

The whole value proposition is "survives a crash", so that is what gets tested hardest:

- `kill -9` harness: a child process writes a known sequence, is killed at a random point,
  is reopened, and the recovered log must be a prefix of what was written with no gaps and
  no records lost below the last successful ack.
- fsync fault injection: a `syncer` interface so tests can fail, delay, or silently drop
  syncs.
- Property tests over the record/segment layer: arbitrary payload sizes, arbitrary
  truncation points, arbitrary bit flips → never panic, never return a bad record.
- Race detector and a concurrent-writers throughput benchmark in CI.

## Measured

`internal/wal`, Apple M5 Pro, APFS, 24-byte records. Go's `File.Sync` on darwin is
`F_FULLFSYNC`, which really does hit the device, so these are pessimistic next to Linux.

| Benchmark | ns/op | records/s |
| --- | --- | --- |
| serial append, `SyncAlways` | 3,102,000 | ~320 |
| serial append, `SyncPeriodic(5ms)` | 5,070,000 | ~200 |
| serial append, `SyncNever` | 1,912 | ~523,000 |
| parallel append, `SyncPeriodic(2ms)`, 18 procs | 329,900 | ~3,000 |
| `AppendBatch(100)`, `SyncAlways` | 3,208,000 | ~31,000 |
| `AppendBatch(1000)`, `SyncAlways` | 3,104,000 | ~322,000 |

Every fsync-ing row costs the same ~3.1ms per commit round regardless of how much it
carries, which is the whole point:

> **Throughput is records-per-commit-round divided by fsync latency.** The only levers are
> raising the numerator (more concurrent writers, or `AppendBatch`) or accepting a weaker
> sync mode. A lone writer calling `Append` in a loop cannot go faster than the device.

The `Buffer[T]` layer should therefore feed the log with `AppendBatch` wherever the caller
hands it more than one event, and `WriteBatch` should be the documented fast path.

Benchmarking this also caught a design bug worth remembering: `SyncNever` originally waited
for the commit ticker like `SyncPeriodic`, so it paid 5ms of latency to avoid an fsync it
was never going to do. Only `SyncPeriodic` should delay; the other two modes have nothing
to amortise. That was a 2,600x difference.

`Buffer[T]` on top of it, same machine, with a no-op sink:

| Benchmark | ns/op | records/s | vs. raw log |
| --- | --- | --- | --- |
| `Write`, `SyncNever` | 4,204 | ~240,000 | 2.2x the log's per-record cost |
| `WriteBatch(100)`, `SyncAlways` | 3,653,000 | ~27,000 | 0.88x |
| `WriteBatch(1000)`, `SyncAlways` | 3,624,000 | ~277,000 | 0.86x |

The batching layer costs about 14% on the batch path and rather more per single `Write`,
where encoding and capacity accounting are no longer hidden behind an fsync.

Two bugs the tests caught, both worth keeping in mind for the layers still to come:

- **`DropOldest` could not free capacity held by an in-flight batch.** Raising the drop
  floor only affected records the flusher had not reached yet, so with the whole backlog
  dispatched to a failing sink, a writer blocked forever under a policy whose entire job is
  to never block. A batch in flight can only be abandoned whole, so that is what it does
  now — coarser than dropping single records, and the reason it is documented as
  approximate. Raising the floor also cuts that batch's retry backoff short, since the
  abandon is what hands its capacity back and the backoff can have grown to half a minute.
- **`Close` closed the log while the flusher was still reading it.** Only reachable when
  `Close`'s context expired first, which is exactly when a shutdown is already going badly.
  `Close` now waits for the flusher to actually stop before touching the log, always.

Five more came out of a review pass before the first release. What they have in common is
that each one is a case where two things that look alike are not:

- **A rolled-back record could be reported as durable.** A waiter compared its LSN against
  the durable mark before checking whether its round had failed, and a rollback hands the
  LSNs straight back to the next round. Once a replacement took the number, the number said
  "durable" and the original writer was told its lost record was safe. Outcomes now belong
  to the round, not to the number.
- **Lowering `MaxRecordBytes` deleted records.** Recovery read any over-long length field as
  a corrupt one and truncated the tail, so a smaller limit on the next `Open` silently
  removed everything from the first record that no longer fit. The checksum tells a real
  record from a corrupt length, so recovery reads it and refuses to open instead.
- **A cancelled shutdown counted as retries running out.** `Close`'s deadline cancels the
  delivery context; the sink returned that error like any other, and a batch on its last
  attempt was dead-lettered or dropped and then checkpointed away. Cancellation from the
  shutdown now leaves the range unacknowledged, which is what makes "nothing is lost when
  `Close` times out" true.
- **`BlockThenDropOldest` could block for ever.** The policy is only consulted when the
  admission loop wakes, and the loop only woke when capacity was released — which a dead
  sink never does. A policy that gives up after a grace period needs a clock of its own, so
  the loop now waits on a short, doubling timer as well.
- **Capacity did not bound what the log kept.** Capacity was released when a batch was
  acknowledged, but a batch acknowledged ahead of an unfinished earlier one cannot move the
  low-water mark, and nothing below the mark can be truncated. With `MaxInFlight` above 1,
  one stuck batch let writes carry on for ever while the log grew. Capacity is now released
  as the mark advances, which is the moment the space can actually come back.

A follow-up review found three more, two of them in that round's own work. The theme is the
same — two things that look alike and are not:

- **A cancelled dead-letter call still counted as a verdict.** The shutdown check went into
  the primary flush path but not into the dead-letter one, so a `Close` deadline that
  cancelled the dead-letter sink dropped the batch and checkpointed past it. Both calls ask
  the same question before disposing of anything now.
- **Dropped records were exempted from that same accounting.** Skipped records — policy
  drops, undecodable ones — released their capacity the moment the flusher read past them,
  on the reasoning that the mark would follow immediately. It does not when something
  earlier is stuck, which is precisely the case the accounting exists for, so the flusher
  carries their counts into the range that acknowledges them. Exempting the easy case from
  an invariant is how the invariant stops being one.
- **The oversized-record check could not tell a read failure from a bad checksum.** The
  helper that streams a too-long payload through the CRC turned every error into "not a
  record", and recovery reads that as a torn tail — so an EIO would have truncated the
  segment. It returns the error separately now, the way the neighbouring read already did.

### Where the time actually goes

Profiling `Write` under `SyncNever` put 61% of CPU in syscalls, and 23% of the total in the
*flusher* — `Reader.Next` was issuing two `pread` calls per record to read back data the
process had just written. Windowing those reads was the single largest win so far:

| | before | after |
| --- | --- | --- |
| `Write`, `SyncNever` | 4,204 ns (~240k/s) | **2,900 ns (~345k/s)** |
| `WriteBatch(1000)`, `SyncAlways` | ~277k/s | ~265-290k/s (fsync-bound, noisy) |
| flusher share of CPU | 23.3% | 13.0% |

What remains is a `write(2)` per serial `Write`, and that is not a defect to optimise away:
it is what "return once the record is durable" costs when only one record is pending. Group
commit can only amortise a syscall across writers that are actually concurrent.

### Ephemeral mode: recommend dropping it

Roadmap item 4 was a memory-first mode that spills to disk only under pressure. The numbers
argue against building it:

- `SyncNever` already runs at ~345k records/s single-threaded **and still survives process
  death**, because the page cache outlives the process. Only a machine crash loses anything.
- The remaining cost is the `write(2)` per `Write`, so a pure-memory path is worth roughly
  2x on that one microbenchmark.
- `WriteBatch(1000)` reaches ~275k records/s *with a full fsync per batch*. A caller who
  cares about throughput batches, and batching amortises the syscall to nothing. Ephemeral
  mode optimises the single-record-in-a-tight-loop pattern that such a caller should not be
  using.

Against 2x on the wrong benchmark: a second flusher source, ordering rules between spilled
and in-memory records, a second set of crash semantics, and double the test surface — all to
weaken the one guarantee the library exists to make. `SyncNever` is the fast mode.

## Roadmap

1. ~~`internal/wal` — segmented log, group commit, recovery, reader, checkpoint.~~ **done**
2. ~~`Buffer[T]` — batcher, flusher, sink retry, checkpoint advance.~~ **done**
3. ~~Backpressure policies + capacity accounting, including disk-full.~~ **done**
4. ~~`Ephemeral` mode.~~ **dropped** — see *Measured*; `SyncNever` covers it.
5. ~~Observability.~~ **done** — `Stats` plus `Observer` callbacks. The dependency question
   settled itself: the callbacks carry everything, so Prometheus and OTel wiring lives in
   the README instead of in the module.
6. Sinks worth shipping: ClickHouse, Kafka, OTLP, plus `func` adapters. ← next
