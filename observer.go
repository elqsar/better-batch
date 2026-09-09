package batch

import (
	"sync/atomic"
	"time"
)

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

	// Action is what the buffer decided to do about a failed attempt: retry it,
	// give up on the batch, or stop. Without it a rejection and an outage are
	// the same increment on the same counter, and the two want opposite
	// responses from whoever is paged. It is RetryBatch and carries no meaning
	// when Err is nil.
	Action RetryAction

	// Duration is how long the sink call took.
	Duration time.Duration

	// Err is nil when the sink accepted the batch.
	Err error
}

// Observer receives callbacks as the buffer works. Every field is optional: a
// nil field is skipped, so an Observer that fills in one hook costs nothing for
// the rest.
//
// Callbacks run on whichever goroutine reached the event, and no lock of the
// buffer's is held while they do. OnWrite and OnBackpressure usually run on the
// caller's own goroutine; OnFlush, OnDeadLetter and most OnDrop events run on a
// delivery goroutine; OnDrop also fires from the flusher, and OnCheckpoint from
// the checkpointer or from whoever called Close.
//
// Two consequences follow, and both are easy to get wrong:
//
// Blocking in a callback blocks that goroutine. From the flusher it stops the
// pipeline; from a delivery it also holds one of the MaxInFlight slots, which at
// the default of 1 stops the flusher too. It defeats Close's context as well:
// Close waits for writers and deliveries unconditionally, because closing the
// log while it is still being read would be worse than waiting.
//
// Calling back into the Buffer deadlocks, with one exception. Stats is safe from
// any callback. Write, Flush and Close are not: a batch's capacity and its place
// in the low-water mark are only released after the callback returns, so a hook
// that waits on either waits on itself.
//
// WithAsyncObserver lifts both restrictions by running the callbacks on a
// goroutine of the buffer's own. Without it, keep them to incrementing a counter
// or observing a histogram.
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

// dispatcher runs Observer callbacks on a goroutine of the buffer's own, so
// that a hook which blocks or re-enters the Buffer cannot stall the pipeline or
// deadlock against it. It exists only when WithAsyncObserver asked for it; a nil
// dispatcher means the callbacks run inline, which is the default.
type dispatcher struct {
	ch      chan func()
	done    chan struct{}
	dropped atomic.Uint64
}

func newDispatcher(queue int) *dispatcher {
	d := &dispatcher{ch: make(chan func(), queue), done: make(chan struct{})}
	go d.run()
	return d
}

// post hands f to the dispatcher, dropping it rather than blocking when the
// queue is full: a hook that cannot keep up must cost metrics, never
// throughput. Blocking here would reinstate the very problem this exists for.
func (d *dispatcher) post(f func()) {
	select {
	case d.ch <- f:
	default:
		d.dropped.Add(1)
	}
}

func (d *dispatcher) run() {
	defer close(d.done)
	for f := range d.ch {
		d.call(f)
	}
}

// call recovers, because one panicking hook must not silently take every metric
// after it down with the goroutine. The count lands in Stats().ObserverDropped,
// so it shows up rather than being swallowed. Synchronous callbacks are left
// alone: there the panic reaches the caller's own stack, where it belongs.
func (d *dispatcher) call(f func()) {
	defer func() {
		if recover() != nil {
			d.dropped.Add(1)
		}
	}()
	f()
}

func (d *dispatcher) stop() { close(d.ch) }

// emit helpers keep the counter and the callback in one place, so a new drop
// site cannot update one and forget the other.
//
// Each one builds its closure only on the asynchronous branch. Constructing it
// unconditionally would make it escape to the heap on the synchronous path too,
// which is the default and is meant to cost exactly what a direct call costs.

func (b *Buffer[T]) dropped(records int, reason DropReason) {
	b.counters.dropped.Add(uint64(records))
	f := b.cfg.observer.OnDrop
	if f == nil {
		return
	}
	if b.obs == nil {
		f(records, reason)
		return
	}
	b.obs.post(func() { f(records, reason) })
}

func (b *Buffer[T]) wrote(records, bytes int) {
	b.counters.written.Add(uint64(records))
	f := b.cfg.observer.OnWrite
	if f == nil {
		return
	}
	if b.obs == nil {
		f(records, bytes)
		return
	}
	b.obs.post(func() { f(records, bytes) })
}

func (b *Buffer[T]) observeFlush(info FlushInfo) {
	f := b.cfg.observer.OnFlush
	if f == nil {
		return
	}
	if b.obs == nil {
		f(info)
		return
	}
	b.obs.post(func() { f(info) })
}

func (b *Buffer[T]) observeBackpressure(s State, d Decision) {
	f := b.cfg.observer.OnBackpressure
	if f == nil {
		return
	}
	if b.obs == nil {
		f(s, d)
		return
	}
	b.obs.post(func() { f(s, d) })
}

func (b *Buffer[T]) deadLettered(records int) {
	b.counters.deadLettered.Add(uint64(records))
	f := b.cfg.observer.OnDeadLetter
	if f == nil {
		return
	}
	if b.obs == nil {
		f(records)
		return
	}
	b.obs.post(func() { f(records) })
}

func (b *Buffer[T]) checkpointed(lsn uint64) {
	f := b.cfg.observer.OnCheckpoint
	if f == nil {
		return
	}
	if b.obs == nil {
		f(lsn)
		return
	}
	b.obs.post(func() { f(lsn) })
}
