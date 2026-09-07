package batch

import (
	"time"

	"github.com/elqsar/better-batch/internal/wal"
)

// SyncMode controls when accepted records are forced to stable storage.
type SyncMode int

const (
	// SyncPeriodic fsyncs on a timer or once enough bytes have accumulated, and
	// Write returns once that fsync is done. The interval is therefore latency a
	// write may wait, not data it may lose: it is how long the buffer holds out
	// for company, so that concurrent writes share one fsync. The default.
	SyncPeriodic SyncMode = iota

	// SyncAlways commits as soon as there is something to commit, trading
	// throughput for latency. Concurrent writers still share one fsync, so
	// throughput comes from concurrency and WriteBatch.
	SyncAlways

	// SyncNever hands records to the operating system but never fsyncs. A
	// process crash loses nothing; a machine crash may lose recent writes.
	SyncNever
)

type config struct {
	wal wal.Options

	maxRecords int64
	maxBytes   int64

	flushRecords  int
	flushBytes    int
	flushInterval time.Duration

	maxInFlight int
	policy      Policy

	backoff     Backoff
	maxAttempts int
	deadLetter  any

	checkpointInterval time.Duration
	observer           Observer
}

func defaults() config {
	return config{
		maxRecords:         1_000_000,
		maxBytes:           1 << 30,
		flushRecords:       1000,
		flushBytes:         4 << 20,
		flushInterval:      time.Second,
		maxInFlight:        1,
		policy:             Block(),
		backoff:            Backoff{Jitter: 0.2}.withDefaults(),
		maxAttempts:        0, // retry forever; a durable buffer should not drop by default
		checkpointInterval: 200 * time.Millisecond,
	}
}

// Option configures a Buffer.
type Option func(*config)

// WithSync selects the durability policy. interval applies to SyncPeriodic and
// bounds how long a write waits to be committed with others; it defaults to 5ms.
func WithSync(mode SyncMode, interval time.Duration) Option {
	return func(c *config) {
		c.wal.SyncMode = walSyncMode(mode)
		c.wal.SyncInterval = interval
	}
}

// walSyncMode maps the public mode explicitly rather than converting the
// integer, so reordering either set of constants cannot silently change what a
// caller asked for.
func walSyncMode(m SyncMode) wal.SyncMode {
	switch m {
	case SyncAlways:
		return wal.SyncAlways
	case SyncNever:
		return wal.SyncNever
	default:
		return wal.SyncPeriodic
	}
}

// WithSegmentBytes sets the soft cap on log segment size. Rotation happens
// between commit rounds, so a segment may overshoot by up to one commit batch.
func WithSegmentBytes(n int64) Option {
	return func(c *config) { c.wal.MaxSegmentBytes = n }
}

// WithMaxRecordBytes caps the encoded size of a single record.
func WithMaxRecordBytes(n int) Option {
	return func(c *config) { c.wal.MaxRecordBytes = n }
}

// WithCapacity bounds the backlog the log still holds. Exceeding either limit
// hands the write to the configured Policy. Zero means unlimited, which makes
// the sink the only thing standing between a burst and a full disk.
//
// The backlog is measured against the low-water mark rather than against what
// the sink has taken, because that is what the disk holds: records behind a
// batch that has not finished keep their capacity until it lands and the log
// can be truncated past them. That includes records nobody will deliver — ones
// a policy dropped, or ones that would not decode — since those sit in the log
// like any other until the mark passes them.
func WithCapacity(maxRecords int64, maxBytes int64) Option {
	return func(c *config) {
		c.maxRecords = maxRecords
		c.maxBytes = maxBytes
	}
}

// WithFlush sets the batch-out triggers: a batch is dispatched once it holds
// maxRecords records or maxBytes of encoded payload, or once maxInterval has
// passed with anything buffered. Zero leaves a trigger at its default.
func WithFlush(maxRecords, maxBytes int, maxInterval time.Duration) Option {
	return func(c *config) {
		if maxRecords > 0 {
			c.flushRecords = maxRecords
		}
		if maxBytes > 0 {
			c.flushBytes = maxBytes
		}
		if maxInterval > 0 {
			c.flushInterval = maxInterval
		}
	}
}

// WithMaxInFlight allows n batches to be delivered concurrently. The default of
// 1 preserves order; above that, batches can reach the sink out of order and
// the sink must be safe for concurrent use.
func WithMaxInFlight(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxInFlight = n
		}
	}
}

// WithOnFull sets the backpressure policy. The default is Block.
func WithOnFull(p Policy) Option {
	return func(c *config) {
		if p != nil {
			c.policy = p
		}
	}
}

// WithRetry configures flush retries. maxAttempts of 0 retries forever, which
// is the default: a durable buffer that drops data on a transient outage is not
// doing its job. With a positive maxAttempts, an exhausted batch goes to the
// dead-letter sink, or is dropped and counted if there is none.
//
// A zero field in b takes its default, except Jitter, where zero means no
// randomisation. The buffer's own default is 0.2, which is what stops a fleet
// that shared an outage from retrying in lockstep.
func WithRetry(b Backoff, maxAttempts int) Option {
	return func(c *config) {
		c.backoff = b.withDefaults()
		c.maxAttempts = maxAttempts
	}
}

// WithDeadLetter sets where batches go once they have exhausted their retries.
func WithDeadLetter[T any](s Sink[T]) Option {
	return func(c *config) { c.deadLetter = s }
}

// WithObserver attaches callbacks for the events the buffer emits: writes,
// flush attempts, drops, dead letters, backpressure decisions and checkpoints.
//
// It is how metrics get out without the library depending on any particular
// metrics package. Every field of the Observer is optional. See the package
// README for worked Prometheus and OpenTelemetry wiring.
func WithObserver(o Observer) Option {
	return func(c *config) { c.observer = o }
}

// WithCheckpointInterval sets how often the low-water mark is persisted.
//
// The checkpoint is an optimisation, not a correctness requirement: a crash
// replays from the last one, so a longer interval trades duplicate deliveries
// after a crash for fewer fsyncs during normal running.
func WithCheckpointInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.checkpointInterval = d
		}
	}
}
