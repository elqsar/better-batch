// Package batch provides high-throughput ingest with a durable buffer: write
// single events in, get batches out, survive a crash, and choose what happens
// when the sink can't keep up.
//
// A [Buffer] persists every accepted record to a segmented write-ahead log
// before Write returns, then assembles records into batches by count, byte
// size, or time, and delivers them to a [Sink]. Records are released — and
// their log space reclaimed — only after the sink acknowledges them, giving
// at-least-once delivery across process crashes. Records in a batch are
// consecutive and numbered from [Batch.ID], and a number is never reused, so a
// sink that treats ID+i as an idempotency key gets effectively-once delivery.
//
// Backpressure is pluggable through the [Policy] interface: block, reject,
// drop newest, drop oldest, or block with a deadline and then shed. A full
// disk flows through the same policy, flagged by [State.DiskFull].
//
// The package has no dependencies outside the standard library. Metrics and
// tracing hook in through [Observer] callbacks and [Buffer.Stats].
//
// This is a library, not a daemon and not a broker: it embeds in the process
// producing the events, and one buffer directory belongs to one process at a
// time.
package batch
