package batch

import "time"

// DropReason says why records were discarded.
type DropReason int

const (
	// ReasonPolicy: the backpressure policy chose to shed them, either as
	// DecisionDropNewest on the way in or DecisionDropOldest from the backlog.
	ReasonPolicy DropReason = iota

	// ReasonDecode: the codec could not decode a record read back from the log.
	// It can never be delivered, so it is discarded rather than wedging the
	// pipeline behind it.
	ReasonDecode

	// ReasonRetriesExhausted: the batch ran out of attempts and there was no
	// dead-letter sink, or the dead-letter sink failed too.
	ReasonRetriesExhausted
)

func (r DropReason) String() string {
	switch r {
	case ReasonPolicy:
		return "policy"
	case ReasonDecode:
		return "decode"
	case ReasonRetriesExhausted:
		return "retries_exhausted"
	default:
		return "unknown"
	}
}

// FlushInfo describes one attempt to hand a batch to a sink. It is reported for
// failures as well as successes, so the same hook feeds a latency histogram and
// an error counter.
type FlushInfo struct {
	// ID is the batch's stable identifier, the same value as Batch.ID. It does
	// not change between attempts.
	ID uint64

	Records int
	Bytes   int

	// Attempt counts from 1. The dead-letter sink gets its own count, starting
	// over from 1 once the primary sink's attempts are exhausted.
	Attempt int

	// DeadLetter is true when this attempt was against the dead-letter sink
	// rather than the primary one.
	DeadLetter bool

	// Duration is how long the sink call took.
	Duration time.Duration

	// Err is nil when the sink accepted the batch.
	Err error
}

// Observer receives callbacks as the buffer works. Every field is optional: a
// nil field is skipped, so an Observer that fills in one hook costs nothing for
// the rest.
//
// Callbacks run on whichever goroutine reached the event: OnWrite and
// OnBackpressure usually run on the caller's, the rest on the buffer's own.
// Blocking in one blocks that goroutine — the pipeline, or a write — and calling
// back into the Buffer from one will deadlock, so keep them to incrementing a
// counter or observing a histogram.
//
// Gauges — backlog depth, checkpoint lag, pending bytes — come from Stats
// instead, which is meant to be polled. Use the Observer for things that happen
// and Stats for things that are.
type Observer struct {
	// OnWrite fires once per accepted Write or WriteBatch, with the number of
	// records and their encoded size. A write that returned ErrUncertain fires
	// it later, from the buffer's own goroutine, if the records turn out to have
	// been committed after all.
	OnWrite func(records, bytes int)

	// OnFlush fires after every sink attempt, successful or not.
	OnFlush func(FlushInfo)

	// OnDrop fires whenever records are discarded and will never be delivered.
	OnDrop func(records int, reason DropReason)

	// OnDeadLetter fires when a batch that exhausted its retries was accepted
	// by the dead-letter sink.
	OnDeadLetter func(records int)

	// OnBackpressure fires each time the policy is consulted, with the state it
	// saw and the decision it made. State.DiskFull distinguishes a full
	// filesystem from a full buffer.
	OnBackpressure func(State, Decision)

	// OnCheckpoint fires when the low-water mark is persisted and the segments
	// below it are dropped.
	OnCheckpoint func(lsn uint64)
}

// emit helpers keep the counter and the callback in one place, so a new drop
// site cannot update one and forget the other.

func (b *Buffer[T]) dropped(records int, reason DropReason) {
	b.counters.dropped.Add(uint64(records))
	if f := b.cfg.observer.OnDrop; f != nil {
		f(records, reason)
	}
}

func (b *Buffer[T]) wrote(records, bytes int) {
	b.counters.written.Add(uint64(records))
	if f := b.cfg.observer.OnWrite; f != nil {
		f(records, bytes)
	}
}

func (b *Buffer[T]) observeFlush(info FlushInfo) {
	if f := b.cfg.observer.OnFlush; f != nil {
		f(info)
	}
}

func (b *Buffer[T]) observeBackpressure(s State, d Decision) {
	if f := b.cfg.observer.OnBackpressure; f != nil {
		f(s, d)
	}
}
