package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/elqsar/better-batch/internal/wal"
)

// Errors returned by a Buffer.
var (
	// ErrFull means the buffer is at capacity and the policy chose to reject.
	ErrFull = errors.New("batch: buffer is full")

	// ErrClosed means the buffer is closing or closed and accepts no writes.
	ErrClosed = errors.New("batch: buffer is closed")

	// ErrDiskFull means the log could not grow and the policy chose to reject.
	// The buffer stays usable: space returns as the sink drains and segments
	// are truncated.
	ErrDiskFull = errors.New("batch: no space left on device")

	// ErrUncertain means the write's context expired while its records were
	// waiting for their commit round. They were staged, so they may still be
	// committed and delivered to the sink; the buffer finishes accounting for
	// them either way. Retrying produces a duplicate, which at-least-once
	// delivery already allows.
	ErrUncertain = errors.New("batch: write outcome uncertain")
)

// isDiskFull reports whether an error is the filesystem being out of room.
func isDiskFull(err error) bool { return errors.Is(err, syscall.ENOSPC) }

// Buffer accepts records at high rates, keeps them durably, and hands them to a
// sink in batches. It is safe for concurrent use.
type Buffer[T any] struct {
	dir   string
	log   *wal.Log
	sink  Sink[T]
	dlq   Sink[T]
	codec Codec[T]
	cfg   config

	acks *ackTracker

	// Accounting for the unflushed backlog, which is what capacity limits and
	// the backpressure policy are measured against.
	pendingRecords atomic.Int64
	pendingBytes   atomic.Int64
	ages           ageTracker // age of the oldest unflushed record
	sinkFailures   atomic.Int64

	// floor is the LSN below which records have been abandoned by DropOldest.
	floor atomic.Uint64

	counters struct {
		written, flushed, dropped, deadLettered, retries, diskFull atomic.Uint64
	}

	space    *gate // signalled when the backlog shrinks
	progress *gate // signalled when the low-water mark advances
	floors   *gate // signalled when DropOldest raises the floor

	wake chan struct{} // nudges the flusher
	sem  chan struct{} // bounds concurrent deliveries

	mu      sync.Mutex // guards sealed and the writers WaitGroup
	sealed  bool
	writers sync.WaitGroup

	closing  chan struct{} // writers stop waiting for space
	drainNow chan struct{} // flusher drains what is left and exits
	abort    chan struct{} // deliveries stop retrying
	drained  chan struct{} // closed once the flusher has exited

	deliveries sync.WaitGroup
	cpStop     chan struct{}
	cpDone     chan struct{}

	// deliverCtx is cancelled when Close gives up waiting, so a sink that
	// honours its context stops instead of hanging the shutdown.
	deliverCtx    context.Context
	deliverCancel context.CancelFunc

	closeOnce sync.Once
	closeErr  error // set inside closeOnce; every Close returns it
	lastCP    atomic.Uint64
	fatalErr  atomic.Pointer[error]
	cpErr     atomic.Pointer[error]

	// forceFlush makes the flusher dispatch a partial batch on its next wake,
	// so Flush is not left waiting out the flush interval.
	forceFlush atomic.Bool
}

// Open creates or reopens a buffer in dir. Any records left over from a previous
// run are replayed to the sink before new ones.
func Open[T any](dir string, sink Sink[T], codec Codec[T], opts ...Option) (*Buffer[T], error) {
	if sink == nil {
		return nil, errors.New("batch: sink is nil")
	}
	if codec == nil {
		return nil, errors.New("batch: codec is nil")
	}
	cfg := defaults()
	for _, o := range opts {
		o(&cfg)
	}

	var dlq Sink[T]
	if cfg.deadLetter != nil {
		s, ok := cfg.deadLetter.(Sink[T])
		if !ok {
			return nil, fmt.Errorf("batch: dead-letter sink has type %T, want Sink[T] of the buffer's record type", cfg.deadLetter)
		}
		dlq = s
	}

	log, err := wal.Open(dir, cfg.wal)
	if err != nil {
		return nil, err
	}
	checkpoint, err := wal.ReadCheckpoint(dir)
	if err != nil {
		log.Close()
		return nil, err
	}
	// The mark has to name a place the log can actually resume from, and it can
	// arrive outside that range from either end.
	//
	// Too high: the checkpoint is always fsynced while SyncNever records are not,
	// so after a machine crash it can point past everything that survived.
	// Starting the flusher above every LSN the log is about to hand out would
	// leave new writes unread until capacity wedged.
	//
	// Too low: records at or below the mark have been delivered and deleted, and
	// asking to read them back gets ErrTruncated rather than a fresh start.
	// Nothing is lost either way — a mark of N means every LSN at or below N was
	// already acked, and nothing below the log's first LSN exists to deliver.
	checkpoint = min(checkpoint, log.DurableLSN())
	if first := log.FirstLSN(); first > 0 {
		checkpoint = max(checkpoint, first-1)
	}
	// Everything at or below the checkpoint is already downstream.
	if err := log.Truncate(checkpoint + 1); err != nil {
		log.Close()
		return nil, err
	}

	b := &Buffer[T]{
		dir:      dir,
		log:      log,
		sink:     sink,
		dlq:      dlq,
		codec:    codec,
		cfg:      cfg,
		acks:     newAckTracker(checkpoint),
		space:    newGate(),
		progress: newGate(),
		floors:   newGate(),
		wake:     make(chan struct{}, 1),
		sem:      make(chan struct{}, cfg.maxInFlight),
		closing:  make(chan struct{}),
		drainNow: make(chan struct{}),
		abort:    make(chan struct{}),
		drained:  make(chan struct{}),
		cpStop:   make(chan struct{}),
		cpDone:   make(chan struct{}),
	}
	b.deliverCtx, b.deliverCancel = context.WithCancel(context.Background())
	b.lastCP.Store(checkpoint)
	b.floor.Store(checkpoint + 1)

	// Capacity limits have to account for a backlog inherited from the previous
	// run, or a restart would admit a second bufferful on top of it.
	if err := b.countBacklog(checkpoint + 1); err != nil {
		log.Close()
		return nil, err
	}

	go b.flusher(checkpoint + 1)
	go b.checkpointer()
	return b, nil
}

func (b *Buffer[T]) countBacklog(from uint64) error {
	r, err := b.log.NewReader(from)
	if err != nil {
		if errors.Is(err, wal.ErrTruncated) {
			return nil
		}
		return err
	}
	defer r.Close()
	var records, bytes int64
	var last uint64
	for {
		lsn, payload, err := r.Next()
		if errors.Is(err, wal.ErrNoData) {
			break
		}
		if err != nil {
			return err
		}
		last = lsn
		records++
		bytes += int64(len(payload))
	}
	b.pendingRecords.Store(records)
	b.pendingBytes.Store(bytes)
	if records > 0 {
		// The true write times predate this process; dating the inherited
		// backlog from Open is the conservative choice available.
		b.ages.note(last, time.Now().UnixNano())
	}
	return nil
}

// Write appends a single record. It returns once the record is durable, or once
// the backpressure policy has decided what to do about a full buffer.
func (b *Buffer[T]) Write(ctx context.Context, v T) error {
	return b.WriteBatch(ctx, v)
}

// WriteBatch appends several records under one commit round and one capacity
// decision. It is the fast path: the cost of durability is dominated by the
// fsync, so the more records share one, the higher the throughput.
//
// Either all of the records are accepted or none are.
//
// If ctx expires after the records have been staged but before their commit
// round finishes, WriteBatch returns ErrUncertain wrapping the context error:
// the records may still be committed and delivered. The buffer settles its own
// accounting either way, so the caller's only decision is whether to retry and
// accept a possible duplicate.
func (b *Buffer[T]) WriteBatch(ctx context.Context, vs ...T) error {
	if len(vs) == 0 {
		return nil
	}
	if err := b.enter(); err != nil {
		return err
	}
	defer b.writers.Done()

	var enc []byte
	ends := make([]int, len(vs))
	for i, v := range vs {
		var err error
		enc, err = b.codec.Encode(enc, v)
		if err != nil {
			return fmt.Errorf("batch: encode record %d: %w", i, err)
		}
		ends[i] = len(enc)
	}
	payloads := make([][]byte, len(vs))
	start := 0
	for i, end := range ends {
		payloads[i] = enc[start:end]
		start = end
	}

	n, size := int64(len(vs)), int64(len(enc))
	for attempt := 1; ; attempt++ {
		admitted, err := b.admit(ctx, n, size)
		if err != nil || !admitted {
			return err
		}
		c, err := b.log.AppendBatchAsync(payloads)
		if err == nil {
			switch err = b.log.Await(ctx, c); {
			case err == nil:
				b.accept(c.Last, n, size)
				return nil
			case errors.Is(err, wal.ErrPending):
				// ctx expired with the commit round still in flight. The records
				// are staged and will very likely land, so the reservation stays
				// put and a settler finishes the accounting once the round
				// completes.
				b.settle(c, n, size)
				return fmt.Errorf("%w: %w", ErrUncertain, err)
			}
		}
		// Both halves can fail on a full disk. Committing obviously can; staging
		// can too, because claiming a block of LSNs writes them down first, and
		// the very first write to a directory does that. Either way the log has
		// rolled back to a state where retrying is safe.
		b.release(n, size)
		if !isDiskFull(err) {
			return fmt.Errorf("batch: append: %w", err)
		}
		// The filesystem is the limit, not the configured capacity. A failed
		// commit is rolled back by the log, so retrying is safe once the
		// flusher has drained enough to truncate a segment away.
		retry, err := b.onDiskFull(ctx, n, size, attempt)
		if err != nil || !retry {
			return err
		}
	}
}

// onDiskFull runs the backpressure policy for a write the log had no room for.
func (b *Buffer[T]) onDiskFull(ctx context.Context, n, size int64, attempt int) (bool, error) {
	b.counters.diskFull.Add(1)

	// Take the gate before deciding, so space freed while the policy runs
	// cannot be missed.
	waiting := b.space.wait()
	st := b.state(attempt)
	st.DiskFull = true

	decision := b.cfg.policy.OnFull(ctx, st)
	b.observeBackpressure(st, decision)

	switch decision {
	case DecisionReject:
		return false, ErrDiskFull
	case DecisionDropNewest:
		b.dropped(int(n), ReasonPolicy)
		return false, nil
	case DecisionDropOldest:
		b.dropOldest(n, size)
	}
	b.nudge()

	// Space can also come back from outside this process, so the wait is
	// bounded rather than relying only on the flusher's own progress.
	timer := time.NewTimer(diskFullRetryInterval)
	defer timer.Stop()
	select {
	case <-waiting:
	case <-timer.C:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-b.closing:
		return false, ErrClosed
	}
	return true, nil
}

// diskFullRetryInterval bounds how long a writer parked on a full disk waits
// before trying the log again.
const diskFullRetryInterval = 100 * time.Millisecond

// blockRetryFloor and blockRetryInterval bound how long a blocked writer waits
// before its policy is consulted again. The wait starts at the floor and
// doubles up to the interval: short enough that a brief grace period is still
// honoured, long enough that a writer parked on a dead sink costs nothing.
const (
	blockRetryFloor    = time.Millisecond
	blockRetryInterval = 100 * time.Millisecond
)

// enter registers a writer, unless the buffer has been sealed by Close.
func (b *Buffer[T]) enter() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return ErrClosed
	}
	b.writers.Add(1)
	return nil
}

// reserve takes capacity if it is available, all or nothing.
func (b *Buffer[T]) reserve(n, size int64) bool {
	records := b.pendingRecords.Add(n)
	bytes := b.pendingBytes.Add(size)
	if (b.cfg.maxRecords > 0 && records > b.cfg.maxRecords) ||
		(b.cfg.maxBytes > 0 && bytes > b.cfg.maxBytes) {
		b.pendingRecords.Add(-n)
		b.pendingBytes.Add(-size)
		return false
	}
	return true
}

// accept records a batch that reached the log as LSNs up to last. Its capacity
// was reserved before the append and stays held until the flusher delivers the
// records.
func (b *Buffer[T]) accept(last uint64, n, size int64) {
	// Note the age before the observer callback: the records are durable, so
	// the flusher may already be delivering them, and OnWrite is the caller's
	// code and free to take as long as it likes.
	b.ages.note(last, time.Now().UnixNano())
	b.wrote(int(n), int(size))
	b.nudge()
}

// settle finishes the accounting for records whose writer stopped waiting for
// the commit round. Their fate is still decided by the log, so somebody has to
// observe it: without this the reservation is either leaked when the round is
// rolled back, or released twice when it succeeds and the flusher delivers.
//
// The caller still holds its own writers slot, so taking another before
// releasing that one keeps the counter above zero and makes Close wait for the
// outcome. The wait is bounded by one commit round.
func (b *Buffer[T]) settle(c wal.Commit, n, size int64) {
	b.writers.Go(func() {
		if err := b.log.Await(context.Background(), c); err != nil {
			b.release(n, size) // rolled back: the records never existed
			return
		}
		b.accept(c.Last, n, size)
	})
}

func (b *Buffer[T]) release(n, size int64) {
	b.pendingRecords.Add(-n)
	b.pendingBytes.Add(-size)
	b.space.signal()
}

// admit applies the backpressure policy until the write fits, is refused, or is
// dropped. A false first return with a nil error means the policy dropped it.
func (b *Buffer[T]) admit(ctx context.Context, n, size int64) (bool, error) {
	wait := blockRetryFloor
	for attempt := 1; ; attempt++ {
		// Take the gate before testing capacity, so a release that happens
		// between the test and the wait cannot be missed.
		waiting := b.space.wait()
		if b.reserve(n, size) {
			return true, nil
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}

		st := b.state(attempt)
		decision := b.cfg.policy.OnFull(ctx, st)
		b.observeBackpressure(st, decision)

		switch decision {
		case DecisionReject:
			return false, ErrFull
		case DecisionDropNewest:
			b.dropped(int(n), ReasonPolicy)
			return false, nil
		case DecisionDropOldest:
			b.dropOldest(n, size)
		}

		// The wait is bounded, not just gated on capacity coming back: a policy
		// that blocks for a while and then sheds is only asked again when this
		// loop wakes, and a sink that has stopped acknowledging anything
		// releases nothing to wake it with.
		timer := time.NewTimer(wait)
		select {
		case <-waiting:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-b.closing:
			timer.Stop()
			return false, ErrClosed
		}
		timer.Stop()
		wait = min(2*wait, blockRetryInterval)
	}
}

// dropOldest abandons the head of the backlog to make room for a new write.
//
// Records still sitting in the log are dropped one by one as the flusher reads
// past them. A batch already in flight can only be dropped whole, because it has
// been handed to the sink as a unit — so under a failing sink this can discard
// more than strictly necessary. That is the trade DropOldest exists to make.
func (b *Buffer[T]) dropOldest(n, size int64) {
	records := b.pendingRecords.Load()
	if records <= 0 {
		return
	}
	avg := max(b.pendingBytes.Load()/records, 1)
	// Free room for this write plus headroom, so a stream of writes against a
	// dead sink does not re-enter the policy for every single record.
	//
	// Counting LSNs off from the mark works because the flusher acknowledges
	// stretches that carry no record as soon as it reaches them: the mark is
	// always just below the oldest record still pending, and the records above
	// it are consecutive.
	want := min(max(n+n/4+1, size/avg), records)
	b.raiseFloor(b.acks.mark() + 1 + uint64(want))
	b.nudge()
}

// raiseFloor moves the drop floor forward, never back: concurrent writers may
// each compute a floor, and the highest has to win.
func (b *Buffer[T]) raiseFloor(to uint64) {
	for {
		cur := b.floor.Load()
		if to <= cur {
			return
		}
		if b.floor.CompareAndSwap(cur, to) {
			// A delivery sitting out its backoff may now be below the floor,
			// and it is the only one that can hand that capacity back.
			b.floors.signal()
			return
		}
	}
}

func (b *Buffer[T]) state(attempt int) State {
	age := b.ages.oldest(time.Now().UnixNano())
	return State{
		Records:      b.pendingRecords.Load(),
		Bytes:        b.pendingBytes.Load(),
		MaxRecords:   b.cfg.maxRecords,
		MaxBytes:     b.cfg.maxBytes,
		OldestAge:    age,
		SinkFailures: b.sinkFailures.Load(),
		Attempt:      attempt,
	}
}

func (b *Buffer[T]) nudge() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
