package batch

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/elqsar/better-batch/internal/wal"
)

// pending is a batch under construction, together with the LSN range it will
// acknowledge. The range covers skipped records too, so the low-water mark
// cannot stall behind a record that was dropped rather than delivered.
type pending[T any] struct {
	records   []T
	id        uint64 // LSN of the first record actually in the batch
	rangeFrom uint64
	rangeTo   uint64
	count     int64
	bytes     int64
}

// flusher reads the log, assembles batches, and hands them to the sink.
func (b *Buffer[T]) flusher(from uint64) {
	defer close(b.drained)

	r, err := b.log.NewReader(from)
	if err != nil {
		b.fail(fmt.Errorf("batch: open log reader: %w", err))
		return
	}
	defer r.Close()

	ticker := time.NewTicker(b.cfg.flushInterval)
	defer ticker.Stop()

	var (
		records   []T
		id        uint64
		wantLSN   uint64 // the LSN that would extend the batch being filled
		rangeFrom = from
		rangeTo   = from - 1
		nbytes    int64
		expired   bool
		draining  bool
		cut       bool // a discontinuity ends this batch early

		// A record read across a discontinuity belongs to the next batch, so it
		// is held over rather than re-read.
		held     T
		heldLSN  uint64
		heldSize int64
		hasHeld  bool
	)

	for {
		if hasHeld && len(records) == 0 {
			records = append(records, held)
			id, wantLSN = heldLSN, heldLSN+1
			nbytes += heldSize
			rangeTo = heldLSN
			hasHeld = false
		}

		for !cut && len(records) < b.cfg.flushRecords && nbytes < int64(b.cfg.flushBytes) {
			lsn, payload, err := r.Next()
			if errors.Is(err, wal.ErrNoData) {
				break
			}
			if err != nil {
				b.fail(fmt.Errorf("batch: read log: %w", err))
				return
			}
			size := int64(len(payload))

			var v T
			skip := false
			if lsn < b.floor.Load() {
				b.dropped(1, ReasonPolicy)
				b.release(1, size)
				skip = true
			} else if decoded, derr := b.codec.Decode(payload); derr != nil {
				// A record that will not decode can never be delivered.
				// Dropping it is the only alternative to wedging the pipeline.
				b.dropped(1, ReasonDecode)
				b.release(1, size)
				skip = true
			} else {
				v = decoded
			}

			// A batch holds consecutive records, so that a sink can derive a
			// per-record idempotency key from Batch.ID plus the record's index.
			// A dropped record, or a gap left behind by recovery, therefore ends
			// the batch rather than opening a hole in it.
			if skip {
				rangeTo = lsn // the range still covers what was dropped
				cut = len(records) > 0
				continue
			}
			if len(records) == 0 {
				id, wantLSN = lsn, lsn
			}
			if lsn != wantLSN {
				held, heldLSN, heldSize, hasHeld = v, lsn, size, true
				cut = true
				continue
			}
			records = append(records, v)
			wantLSN++
			nbytes += size
			rangeTo = lsn
		}

		// Flush wants everything out now; treat the interval as elapsed rather
		// than leaving a partial batch to wait out the ticker.
		if b.forceFlush.Swap(false) {
			expired = true
		}

		full := len(records) >= b.cfg.flushRecords || nbytes >= int64(b.cfg.flushBytes)
		if len(records) > 0 && (full || expired || draining || cut) {
			if !b.dispatch(pending[T]{
				records:   records,
				id:        id,
				rangeFrom: rangeFrom,
				rangeTo:   rangeTo,
				count:     int64(len(records)),
				bytes:     nbytes,
			}) {
				return // aborted: the range stays unacknowledged and replays
			}
			records, nbytes, expired, cut = records[:0], 0, false, false
			rangeFrom = rangeTo + 1
			continue
		}

		if len(records) == 0 && rangeTo >= rangeFrom {
			// The whole range was skipped. Acknowledge it directly.
			b.complete(rangeFrom, rangeTo, 0, 0)
			rangeFrom = rangeTo + 1
			cut = false
			continue
		}

		if draining && len(records) == 0 {
			return
		}

		select {
		case <-b.wake:
		case <-ticker.C:
			expired = true
		case <-b.drainNow:
			draining = true
		case <-b.abort:
			// Close gave up waiting and is about to close the log, so the
			// flusher must stop touching it.
			return
		}
	}
}

// dispatch hands a batch to a delivery goroutine, blocking while MaxInFlight
// deliveries are already outstanding. It returns false if the buffer was
// aborted while waiting, in which case the flusher must stop.
func (b *Buffer[T]) dispatch(p pending[T]) bool {
	// The flusher reuses its slice, so the batch handed to the sink is a copy.
	batch := Batch[T]{ID: p.id, Records: slices.Clone(p.records)}
	p.records = nil

	select {
	case b.sem <- struct{}{}:
	case <-b.abort:
		return false // leave the range unacknowledged; it replays on the next open
	}
	b.deliveries.Go(func() {
		defer func() { <-b.sem }()
		b.deliver(batch, p)
	})
	return true
}

func (b *Buffer[T]) deliver(batch Batch[T], p pending[T]) {
	for attempt := 1; ; attempt++ {
		// The floor is compared against the first record, not against rangeFrom:
		// the range also covers LSNs that carry no record — skipped ones, and
		// the gap recovery leaves — and a floor raised into those would abandon
		// a batch whose records are all above it.
		if p.id < b.floor.Load() {
			// DropOldest cut into this batch while it was in flight. It can
			// only be abandoned whole, which also frees the delivery slot the
			// flusher may be waiting on.
			b.dropped(int(p.count), ReasonPolicy)
			b.complete(p.rangeFrom, p.rangeTo, p.count, p.bytes)
			return
		}
		batch.Attempt = attempt
		started := time.Now()
		err := b.sink.Flush(b.deliverCtx, batch)
		b.observeFlush(FlushInfo{
			ID:       batch.ID,
			Records:  int(p.count),
			Bytes:    int(p.bytes),
			Attempt:  attempt,
			Duration: time.Since(started),
			Err:      err,
		})
		if err == nil {
			b.sinkFailures.Store(0)
			b.counters.flushed.Add(uint64(p.count))
			b.complete(p.rangeFrom, p.rangeTo, p.count, p.bytes)
			return
		}
		b.sinkFailures.Add(1)
		b.counters.retries.Add(1)

		if b.cfg.maxAttempts > 0 && attempt >= b.cfg.maxAttempts {
			b.deadLetter(batch, p)
			return
		}
		timer := time.NewTimer(b.cfg.backoff.delay(attempt))
		select {
		case <-timer.C:
		case <-b.abort:
			timer.Stop()
			return // unacknowledged, so the batch is replayed after restart
		}
	}
}

func (b *Buffer[T]) deadLetter(batch Batch[T], p pending[T]) {
	switch {
	case b.dlq == nil:
		b.dropped(int(p.count), ReasonRetriesExhausted)
	default:
		if err := b.dlq.Flush(b.deliverCtx, batch); err != nil {
			// There is nothing left to try. Counting it beats blocking the
			// pipeline on a dead-letter sink that is also down.
			b.dropped(int(p.count), ReasonRetriesExhausted)
		} else {
			b.counters.deadLettered.Add(uint64(p.count))
			if f := b.cfg.observer.OnDeadLetter; f != nil {
				f(int(p.count))
			}
		}
	}
	b.complete(p.rangeFrom, p.rangeTo, p.count, p.bytes)
}

// complete marks an LSN range as handled and gives its capacity back.
func (b *Buffer[T]) complete(from, to uint64, count, bytes int64) {
	mark := b.acks.ack(from, to)
	b.ages.trim(mark)
	b.release(count, bytes)
	b.progress.signal()
}

func (b *Buffer[T]) checkpointer() {
	defer close(b.cpDone)
	ticker := time.NewTicker(b.cfg.checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.persist()
		case <-b.cpStop:
			return
		}
	}
}

// persist writes the low-water mark and drops the log segments below it.
//
// Neither step is fatal if it fails. On a full disk the checkpoint write is
// exactly what cannot happen, and it is also the only route back to free space,
// so the error is recorded and the next tick tries again. Truncating before the
// checkpoint is durable would lose records on a crash, which is why the order
// cannot be swapped to free space sooner.
func (b *Buffer[T]) persist() {
	low := b.acks.mark()
	if low == 0 || low == b.lastCP.Load() {
		return
	}
	if err := wal.WriteCheckpoint(b.dir, low); err != nil {
		b.noteRetryable(fmt.Errorf("batch: write checkpoint: %w", err))
		return
	}
	if err := b.log.Truncate(low + 1); err != nil {
		b.noteRetryable(fmt.Errorf("batch: truncate log: %w", err))
		return
	}
	// lastCP advances only once truncation has succeeded too. Rewriting the
	// checkpoint on a retry is idempotent, and leaving lastCP behind is what
	// makes the next tick retry a failed truncation instead of skipping it.
	b.lastCP.Store(low)
	b.clearRetryable()
	if f := b.cfg.observer.OnCheckpoint; f != nil {
		f(low)
	}
	// Truncation is what actually returns space to the filesystem, so it is the
	// wakeup a writer parked on a full disk is waiting for.
	b.space.signal()
}

// Flush waits until everything written before the call has been accepted by the
// sink. A partial batch is dispatched immediately rather than waiting out the
// flush interval.
func (b *Buffer[T]) Flush(ctx context.Context) error {
	target := b.log.DurableLSN()
	for {
		waiting := b.progress.wait()
		if b.acks.mark() >= target {
			return b.err()
		}
		if err := b.err(); err != nil {
			return err
		}
		b.forceFlush.Store(true)
		b.nudge()
		select {
		case <-waiting:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops accepting writes, drains what is buffered to the sink, records the
// checkpoint, and releases the directory.
//
// If ctx expires first, Close stops waiting and returns its error. Nothing is
// lost: unacknowledged records stay in the log and are replayed on the next
// Open. A sink that ignores its context can still stall shutdown.
func (b *Buffer[T]) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.sealed = true
		b.mu.Unlock()

		close(b.closing) // release writers parked on the capacity gate
		b.writers.Wait() // let already-accepted writes finish appending
		close(b.drainNow)

		timedOut := false
		select {
		case <-b.drained:
		case <-ctx.Done():
			timedOut = true
		}
		if !timedOut {
			done := make(chan struct{})
			go func() { b.deliveries.Wait(); close(done) }()
			select {
			case <-done:
			case <-ctx.Done():
				timedOut = true
			}
		}

		// Abort unblocks the flusher and any delivery sitting in backoff. The
		// log must not be closed until the flusher has actually stopped
		// reading from it, so wait for it unconditionally.
		close(b.abort)
		b.deliverCancel()
		<-b.drained
		b.deliveries.Wait()

		close(b.cpStop)
		<-b.cpDone
		b.persist()

		err := b.log.Close()
		if err == nil && timedOut {
			err = ctx.Err()
		}
		if ferr := b.err(); err == nil && ferr != nil {
			err = ferr
		}
		b.closeErr = err
	})
	// closeOnce.Do happens-before every later Do return, so closeErr is safe to
	// read here and repeated calls report the first outcome instead of nil.
	return b.closeErr
}

// Stats is a point-in-time view of the buffer, suitable for exporting as
// metrics. The counters are cumulative since Open.
type Stats struct {
	Written      uint64 // records accepted by Write
	Flushed      uint64 // records the sink accepted
	Dropped      uint64 // records discarded by policy, decode failure, or exhausted retries
	DeadLettered uint64 // records handed to the dead-letter sink
	Retries      uint64 // failed flush attempts

	PendingRecords int64 // unflushed backlog
	PendingBytes   int64
	OldestAge      time.Duration

	DiskFullEvents uint64 // writes that found the log unable to grow

	Checkpoint   uint64 // last persisted low-water mark
	DurableLSN   uint64 // highest LSN in the log
	SinkFailures int64  // consecutive failed flushes

	// Err is set once the buffer has hit an unrecoverable error. Records
	// already written stay durable and replay on the next Open.
	Err error

	// CheckpointErr is set while checkpointing or truncation keeps failing,
	// typically because the disk is full. It clears itself once a checkpoint
	// succeeds; the buffer keeps accepting and delivering records meanwhile.
	CheckpointErr error
}

// Stats returns a snapshot of the buffer's counters.
func (b *Buffer[T]) Stats() Stats {
	age := b.ages.oldest(time.Now().UnixNano())
	return Stats{
		Written:        b.counters.written.Load(),
		Flushed:        b.counters.flushed.Load(),
		Dropped:        b.counters.dropped.Load(),
		DeadLettered:   b.counters.deadLettered.Load(),
		Retries:        b.counters.retries.Load(),
		DiskFullEvents: b.counters.diskFull.Load(),
		PendingRecords: b.pendingRecords.Load(),
		PendingBytes:   b.pendingBytes.Load(),
		OldestAge:      age,
		Checkpoint:     b.lastCP.Load(),
		DurableLSN:     b.log.DurableLSN(),
		SinkFailures:   b.sinkFailures.Load(),
		Err:            b.err(),
		CheckpointErr:  b.retryableErr(),
	}
}

// noteRetryable records a failure that the next checkpoint tick will retry.
func (b *Buffer[T]) noteRetryable(err error) { b.cpErr.Store(&err) }

func (b *Buffer[T]) clearRetryable() { b.cpErr.Store(nil) }

func (b *Buffer[T]) retryableErr() error {
	if p := b.cpErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (b *Buffer[T]) fail(err error) { b.fatalErr.CompareAndSwap(nil, &err) }

func (b *Buffer[T]) err() error {
	if p := b.fatalErr.Load(); p != nil {
		return *p
	}
	return nil
}
