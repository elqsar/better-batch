package batch

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type stringCodec struct{}

func (stringCodec) Encode(dst []byte, v string) ([]byte, error) { return append(dst, v...), nil }
func (stringCodec) Decode(src []byte) (string, error)           { return string(src), nil }

// recorder is a Sink that remembers what it was given and can be told to fail.
type recorder struct {
	mu       sync.Mutex
	batches  []Batch[string]
	records  []string
	attempts []time.Time  // when each Flush call arrived
	failures atomic.Int64 // fail this many more flushes
	failAll  atomic.Bool
	delay    atomic.Int64 // nanoseconds to sleep in Flush

	// failWith replaces errSink when set, so a test can drive the retry
	// classification without needing a second Sink implementation.
	failWith atomic.Pointer[error]
}

var errSink = errors.New("sink is down")

func (r *recorder) Flush(ctx context.Context, b Batch[string]) error {
	r.mu.Lock()
	r.attempts = append(r.attempts, time.Now())
	r.mu.Unlock()
	if d := r.delay.Load(); d > 0 {
		select {
		case <-time.After(time.Duration(d)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.failAll.Load() || r.failures.Add(-1) >= 0 {
		if e := r.failWith.Load(); e != nil {
			return *e
		}
		return errSink
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, b)
	r.records = append(r.records, b.Records...)
	return nil
}

// fail makes every later Flush return err.
func (r *recorder) fail(err error) {
	r.failWith.Store(&err)
	r.failAll.Store(true)
}

// calls reports how many times Flush has been called.
func (r *recorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

// gaps reports the interval between consecutive Flush calls.
func (r *recorder) gaps() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []time.Duration
	for i := 1; i < len(r.attempts); i++ {
		out = append(out, r.attempts[i].Sub(r.attempts[i-1]))
	}
	return out
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.records)
}

func (r *recorder) batchIDs() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uint64, len(r.batches))
	for i, b := range r.batches {
		ids[i] = b.ID
	}
	return ids
}

func fast(extra ...Option) []Option {
	return append([]Option{
		WithSync(SyncNever, 0),
		WithFlush(50, 1<<20, 5*time.Millisecond),
		WithCheckpointInterval(5 * time.Millisecond),
	}, extra...)
}

func openBuf(t *testing.T, dir string, sink Sink[string], opts ...Option) *Buffer[string] {
	t.Helper()
	b, err := Open[string](dir, sink, stringCodec{}, opts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return b
}

func closeBuf(t *testing.T, b *Buffer[string]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestWriteReachesSink(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast()...)
	ctx := context.Background()

	want := make([]string, 200)
	for i := range want {
		want[i] = fmt.Sprintf("event-%03d", i)
		if err := b.Write(ctx, want[i]); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	if got := sink.seen(); !slices.Equal(got, want) {
		t.Fatalf("sink saw %d records, want %d (first mismatch matters more than the count)", len(got), len(want))
	}
}

func TestBatchesRespectRecordLimit(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithFlush(10, 0, time.Hour))...)
	ctx := context.Background()

	for i := range 100 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.batches) != 10 {
		t.Fatalf("got %d batches, want 10 batches of 10", len(sink.batches))
	}
	for i, batch := range sink.batches {
		if batch.Len() != 10 {
			t.Fatalf("batch %d holds %d records, want 10", i, batch.Len())
		}
	}
}

func TestFlushIntervalDispatchesPartialBatch(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithFlush(1000, 1<<20, 20*time.Millisecond))...)
	defer closeBuf(t, b)

	if err := b.Write(context.Background(), "lonely"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(sink.seen()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("a single record was never dispatched; the interval trigger did not fire")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestRetriesUntilSinkRecovers(t *testing.T) {
	sink := &recorder{}
	sink.failures.Store(5)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}, 0),
	)...)
	ctx := context.Background()

	for i := range 10 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	if got := len(sink.seen()); got != 10 {
		t.Fatalf("sink kept %d records, want 10", got)
	}
	if s := b.Stats(); s.Retries == 0 {
		t.Fatal("Stats reported no retries even though the sink failed five times")
	}
}

func TestBatchIDIsStableAcrossRetries(t *testing.T) {
	var ids []uint64
	var mu sync.Mutex
	fail := 3
	sink := SinkFunc[string](func(_ context.Context, b Batch[string]) error {
		mu.Lock()
		ids = append(ids, b.ID)
		n := len(ids)
		mu.Unlock()
		if n <= fail {
			return errSink
		}
		return nil
	})
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 0),
	)...)
	ctx := context.Background()
	if err := b.Write(ctx, "once"); err != nil {
		t.Fatal(err)
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	mu.Lock()
	defer mu.Unlock()
	if len(ids) < 2 {
		t.Fatalf("sink was called %d times, want the retries", len(ids))
	}
	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("attempt %d had ID %d, want %d; an unstable ID makes sink-side dedup impossible", i, id, ids[0])
		}
	}
}

func TestCheckpointStopsRedelivery(t *testing.T) {
	dir := t.TempDir()
	sink := &recorder{}
	b := openBuf(t, dir, sink, fast()...)
	ctx := context.Background()

	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	sink2 := &recorder{}
	b2 := openBuf(t, dir, sink2, fast()...)
	if err := b2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	closeBuf(t, b2)

	if got := sink2.seen(); len(got) != 0 {
		t.Fatalf("reopening redelivered %d already-flushed records: %v", len(got), got)
	}
}

func TestUnflushedRecordsReplayAfterReopen(t *testing.T) {
	dir := t.TempDir()
	down := &recorder{}
	down.failAll.Store(true)

	b := openBuf(t, dir, down, fast(
		WithSync(SyncAlways, 0),
		WithRetry(Backoff{Initial: 10 * time.Millisecond, Jitter: 0}, 0),
	)...)
	ctx := context.Background()
	for i := range 10 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	// Give up on the dead sink and shut down; nothing was acknowledged.
	cctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_ = b.Close(cctx)

	if got := len(down.seen()); got != 0 {
		t.Fatalf("a failing sink somehow kept %d records", got)
	}

	up := &recorder{}
	b2 := openBuf(t, dir, up, fast()...)
	fctx, fcancel := context.WithTimeout(ctx, 5*time.Second)
	defer fcancel()
	if err := b2.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b2)

	if got := len(up.seen()); got != 10 {
		t.Fatalf("replayed %d records after reopen, want all 10", got)
	}
}

func TestRejectPolicyReturnsErrFull(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(5, 0),
		WithOnFull(Reject()),
		WithRetry(Backoff{Initial: time.Second, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx := context.Background()
	var full int
	for i := range 50 {
		err := b.Write(ctx, fmt.Sprintf("e%d", i))
		if errors.Is(err, ErrFull) {
			full++
			continue
		}
		if err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	if full == 0 {
		t.Fatal("no write was rejected even though capacity was 5 and the sink was dead")
	}
}

func TestBlockPolicyWaitsForCapacity(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(5, 0),
		WithOnFull(Block()),
		WithRetry(Backoff{Initial: 10 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var blocked error
	for i := range 100 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			blocked = err
			break
		}
	}
	if !errors.Is(blocked, context.DeadlineExceeded) {
		t.Fatalf("Write eventually returned %v, want the caller's deadline; Block must not drop or reject", blocked)
	}

	// Once the sink recovers, the parked capacity is released and writes resume.
	sink.failAll.Store(false)
	wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer wcancel()
	if err := b.Write(wctx, "after-recovery"); err != nil {
		t.Fatalf("Write after the sink recovered: %v", err)
	}
}

func TestDropNewestKeepsWritesFast(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(5, 0),
		WithOnFull(DropNewest()),
		WithRetry(Backoff{Initial: time.Second, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx := context.Background()
	for i := range 100 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("DropNewest must never fail a write, got %v", err)
		}
	}
	if s := b.Stats(); s.Dropped == 0 {
		t.Fatal("Stats.Dropped is 0; writes past capacity must be counted, not lost silently")
	}
}

func TestDropOldestMakesRoomAgainstADeadSink(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(20, 0),
		WithOnFull(DropOldest()),
		WithFlush(5, 0, 5*time.Millisecond),
		WithRetry(Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 300 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d against a dead sink with DropOldest: %v", i, err)
		}
	}
	s := b.Stats()
	if s.Dropped == 0 {
		t.Fatal("nothing was dropped; DropOldest never reclaimed capacity")
	}
	if s.PendingRecords > 20 {
		t.Fatalf("backlog is %d records, above the configured capacity of 20", s.PendingRecords)
	}
}

func TestMaxInFlightAllowsConcurrentDelivery(t *testing.T) {
	var concurrent, peak atomic.Int64
	sink := SinkFunc[string](func(context.Context, Batch[string]) error {
		n := concurrent.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		concurrent.Add(-1)
		return nil
	})
	b := openBuf(t, t.TempDir(), sink, fast(
		WithFlush(5, 0, 5*time.Millisecond),
		WithMaxInFlight(4),
	)...)
	ctx := context.Background()
	for i := range 200 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	if peak.Load() < 2 {
		t.Fatalf("peak concurrency was %d with MaxInFlight=4; batches were delivered serially", peak.Load())
	}
}

func TestDeadLetterCancelledByCloseKeepsRecords(t *testing.T) {
	dir := t.TempDir()
	sink := &recorder{}
	sink.failAll.Store(true)
	var reached atomic.Bool
	// The dead-letter sink honours its context and nothing else, so what ends
	// its call is Close's deadline rather than any verdict on the batch.
	dlq := SinkFunc[string](func(ctx context.Context, _ Batch[string]) error {
		reached.Store(true)
		<-ctx.Done()
		return ctx.Err()
	})
	b := openBuf(t, dir, sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 1),
		WithDeadLetter[string](dlq),
	)...)

	ctx := context.Background()
	want := []string{"e0", "e1", "e2"}
	for _, v := range want {
		if err := b.Write(ctx, v); err != nil {
			t.Fatalf("Write %s: %v", v, err)
		}
	}
	waitFor(t, 5*time.Second, reached.Load)

	cctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := b.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close against a hung dead-letter sink = %v, want its deadline", err)
	}
	if s := b.Stats(); s.Dropped != 0 || s.DeadLettered != 0 {
		t.Fatalf("Stats reports %d dropped and %d dead-lettered; a cancelled dead-letter call has not disposed of anything", s.Dropped, s.DeadLettered)
	}

	up := &recorder{}
	b2 := openBuf(t, dir, up, fast()...)
	defer closeBuf(t, b2)
	waitFor(t, 5*time.Second, func() bool { return len(up.seen()) == len(want) })
	if got := up.seen(); !slices.Equal(got, want) {
		t.Fatalf("after reopening the sink saw %q, want %q: records neither sink took must replay", got, want)
	}
}

func TestSkippedRecordsHoldCapacityUntilTheMarkMoves(t *testing.T) {
	// The first batch never completes, so the mark cannot move and nothing can
	// be truncated. Everything written after it fails to decode, and a record
	// nobody will deliver is still a record in the log: its capacity has to stay
	// held, or writes carry on for ever behind a checkpoint that never moves.
	sink := SinkFunc[string](func(ctx context.Context, batch Batch[string]) error {
		if batch.ID == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	b, err := Open[string](t.TempDir(), sink, poisonCodec{}, fast(
		WithCapacity(4, 0),
		WithFlush(2, 0, 5*time.Millisecond),
		WithMaxInFlight(2),
		WithOnFull(Reject()),
	)...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx := context.Background()
	accepted, full := 0, false
	for i := range 40 {
		v := "poison"
		if i < 2 {
			v = fmt.Sprintf("e%d", i) // the two that make the batch that sticks
		}
		switch err := b.Write(ctx, v); {
		case err == nil:
			accepted++
		case errors.Is(err, ErrFull):
			full = true
		default:
			t.Fatalf("write %d: %v", i, err)
		}
		time.Sleep(5 * time.Millisecond) // let the flusher read and skip
	}

	s := b.Stats()
	if !full {
		t.Fatalf("all %d writes were admitted against a capacity of 4; records that will not decode still occupy the log", accepted)
	}
	if accepted > 4 {
		t.Fatalf("%d writes were admitted against a capacity of 4: a skipped record holds its capacity until the mark passes it", accepted)
	}
	if s.Dropped == 0 {
		t.Fatal("nothing was dropped, so the undecodable records never reached the skip path this test is about")
	}
	if s.Checkpoint != 0 {
		t.Fatalf("Checkpoint = %d while the first batch is still outstanding, so the test is not measuring what it means to", s.Checkpoint)
	}
}

func TestDropOldestDoesNotWaitOutTheBackoff(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(20, 0),
		WithOnFull(DropOldest()),
		WithFlush(5, 0, 5*time.Millisecond),
		// Far longer than this test: the capacity held by the batch in flight
		// has to come back because the floor reached it, not because its retry
		// timer happened to fire.
		WithRetry(Backoff{Initial: time.Minute, Max: time.Minute, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 200 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d against a dead sink with DropOldest: %v; the floor must cut the backoff short", i, err)
		}
	}
	if s := b.Stats(); s.Dropped == 0 {
		t.Fatal("nothing was dropped; DropOldest never reclaimed capacity")
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	dlq := &recorder{}

	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 3),
		WithDeadLetter[string](dlq),
	)...)
	ctx := context.Background()
	for i := range 5 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: a batch that exhausts its retries must still advance the checkpoint: %v", err)
	}
	closeBuf(t, b)

	if got := len(dlq.seen()); got != 5 {
		t.Fatalf("dead-letter sink got %d records, want 5", got)
	}
	if s := b.Stats(); s.DeadLettered != 5 {
		t.Fatalf("Stats.DeadLettered = %d, want 5", s.DeadLettered)
	}
}

func TestWriteAfterCloseIsRejected(t *testing.T) {
	b := openBuf(t, t.TempDir(), &recorder{}, fast()...)
	closeBuf(t, b)
	if err := b.Write(context.Background(), "late"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write after Close = %v, want ErrClosed", err)
	}
}

// A record whose writer stopped waiting is still staged and still commits. The
// buffer has to finish the accounting the writer walked away from: releasing its
// reservation here would let the flusher release it a second time on delivery
// and drive the backlog counters negative.
func TestCancelledWriteSettlesAsAccepted(t *testing.T) {
	sink := &recorder{}
	// A sync interval far longer than the write's deadline guarantees the round
	// is still in flight when the writer gives up.
	b := openBuf(t, t.TempDir(), sink, fast(WithSync(SyncPeriodic, time.Second))...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := b.Write(ctx, "staged")
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("Write cancelled mid-commit = %v, want ErrUncertain", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Write = %v, must still match the context error it was given", err)
	}

	waitFor(t, 5*time.Second, func() bool { return len(sink.seen()) == 1 })

	s := b.Stats()
	if s.PendingRecords != 0 || s.PendingBytes != 0 {
		t.Fatalf("Stats pending = %d records / %d bytes after delivery, want 0/0; "+
			"the reservation was released twice", s.PendingRecords, s.PendingBytes)
	}
	if s.Written != 1 || s.Flushed != 1 {
		t.Fatalf("Stats.Written = %d, Flushed = %d; want 1/1, a delivered record must be counted as written",
			s.Written, s.Flushed)
	}
}

// The other half of the same handoff: when the round the writer abandoned turns
// out to fail, the records never existed and their capacity has to come back, or
// the buffer leaks it for good.
func TestCancelledWriteSettlesAsRolledBack(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)

	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(
		WithSync(SyncPeriodic, 200*time.Millisecond),
		withWriteFault(enospcAfter(&failing)),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Write(ctx, "doomed"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("Write cancelled mid-commit = %v, want ErrUncertain", err)
	}

	waitFor(t, 5*time.Second, func() bool { return b.Stats().PendingRecords == 0 })

	s := b.Stats()
	if s.PendingBytes != 0 {
		t.Fatalf("Stats.PendingBytes = %d after a rolled-back commit, want 0", s.PendingBytes)
	}
	if s.Written != 0 {
		t.Fatalf("Stats.Written = %d, want 0; a rolled-back record was never written", s.Written)
	}
	if got := len(sink.seen()); got != 0 {
		t.Fatalf("sink saw %d records from a commit that never landed", got)
	}
}

// Close has to wait for a settler that is still resolving, both so the record
// reaches the sink and so the writers WaitGroup is never left with an Add racing
// its Wait.
func TestCloseWaitsForAnUnsettledWrite(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithSync(SyncPeriodic, 200*time.Millisecond))...)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.Write(ctx, "unsettled"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("Write cancelled mid-commit = %v, want ErrUncertain", err)
	}
	// No waiting: Close arrives while the commit round is still in flight.
	closeBuf(t, b)

	if got := sink.seen(); len(got) != 1 || got[0] != "unsettled" {
		t.Fatalf("sink saw %v, want [unsettled]; Close must drain a record whose writer walked away", got)
	}
}

// waitFor polls until cond holds, which beats sleeping for a fixed time that is
// either flaky or slow.
func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition still false after %s", limit)
}

func TestCloseDrainsEverythingWritten(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithFlush(1000, 0, time.Hour))...)
	ctx := context.Background()
	for i := range 500 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	closeBuf(t, b) // no Flush: Close alone must drain

	if got := len(sink.seen()); got != 500 {
		t.Fatalf("Close drained %d records, want 500", got)
	}
}

func TestConcurrentWritersAllArrive(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast()...)

	const writers, each = 8, 250
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			for i := range each {
				if err := b.WriteBatch(ctx, fmt.Sprintf("w%d-a%d", w, i), fmt.Sprintf("w%d-b%d", w, i)); err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
			}
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	seen := sink.seen()
	if len(seen) != writers*each*2 {
		t.Fatalf("sink got %d records, want %d", len(seen), writers*each*2)
	}
	uniq := make(map[string]bool, len(seen))
	for _, s := range seen {
		if uniq[s] {
			t.Fatalf("record %q was delivered twice with no crash to justify it", s)
		}
		uniq[s] = true
	}
}

func TestCloseCancellationKeepsRecordsForReplay(t *testing.T) {
	dir := t.TempDir()
	var started atomic.Bool
	// A sink that honours its context and never returns on its own: what ends
	// the delivery is Close's deadline, which is the shutdown talking and not
	// the sink's verdict on the batch.
	stuck := SinkFunc[string](func(ctx context.Context, _ Batch[string]) error {
		started.Store(true)
		<-ctx.Done()
		return ctx.Err()
	})
	b := openBuf(t, dir, stuck, fast(
		// One attempt, so the cancelled flush lands straight on the branch that
		// decides retries are exhausted.
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 1),
	)...)

	ctx := context.Background()
	want := []string{"e0", "e1", "e2"}
	for _, v := range want {
		if err := b.Write(ctx, v); err != nil {
			t.Fatalf("Write %s: %v", v, err)
		}
	}
	waitFor(t, 5*time.Second, started.Load)

	cctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := b.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close against a stuck sink = %v, want its deadline", err)
	}
	if s := b.Stats(); s.Dropped != 0 {
		t.Fatalf("Stats.Dropped = %d after a shutdown deadline; cancelling a delivery is not the sink exhausting its retries", s.Dropped)
	}

	sink := &recorder{}
	b2 := openBuf(t, dir, sink, fast()...)
	defer closeBuf(t, b2)
	waitFor(t, 5*time.Second, func() bool { return len(sink.seen()) == len(want) })
	if got := sink.seen(); !slices.Equal(got, want) {
		t.Fatalf("after reopening the sink saw %q, want %q: unacknowledged records must replay", got, want)
	}
}

func TestBlockThenDropOldestGivesUpAfterGrace(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(5, 0),
		WithOnFull(BlockThenDropOldest(100*time.Millisecond)),
		// Retries never end and never succeed, so an acknowledgement never
		// releases anything: the only thing that can free a blocked writer is
		// the policy being asked again once the backlog has aged past the grace
		// period, and then shedding the batch it finds in flight.
		WithRetry(Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	// Comfortably more than the grace period, so a failure here means the writer
	// was never reconsidered rather than that it was reconsidered too late.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := range 50 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v; the grace period passed, so the policy must shed instead of blocking on", i, err)
		}
	}
	if s := b.Stats(); s.Dropped == 0 {
		t.Fatal("nothing was dropped, so the writes cannot have gone through the shedding path they were supposed to")
	}
}

func TestCapacityBoundsRetainedRecords(t *testing.T) {
	// The first batch never completes, so the low-water mark cannot move and
	// the log cannot be truncated. Later batches succeed, and the capacity they
	// hold has to stay held: their records are still on disk behind the stuck
	// one, and releasing them would let writes carry on without bound.
	sink := SinkFunc[string](func(ctx context.Context, batch Batch[string]) error {
		if batch.ID == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(4, 0),
		WithFlush(2, 0, 5*time.Millisecond),
		WithMaxInFlight(2),
		WithOnFull(Reject()),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx := context.Background()
	accepted, full := 0, false
	for i := range 40 {
		switch err := b.Write(ctx, fmt.Sprintf("e%d", i)); {
		case err == nil:
			accepted++
		case errors.Is(err, ErrFull):
			full = true
		default:
			t.Fatalf("write %d: %v", i, err)
		}
		// Let the flusher dispatch and the sink acknowledge, which is what would
		// hand the capacity back too early.
		time.Sleep(5 * time.Millisecond)
	}

	if !full {
		t.Fatalf("all %d writes were admitted against a capacity of 4; acknowledged records still in the log must keep holding it", accepted)
	}
	if accepted > 4 {
		t.Fatalf("%d writes were admitted against a capacity of 4: capacity has to bound what the log retains, not just what the sink has not seen", accepted)
	}
	if s := b.Stats(); s.Checkpoint != 0 {
		t.Fatalf("Checkpoint = %d while the first batch is still outstanding, so the test is not measuring what it means to", s.Checkpoint)
	}
}

func TestAckTrackerLowWaterMark(t *testing.T) {
	a := newAckTracker(0)

	// Capacity is released as the mark passes a range, not as the range is
	// acknowledged: until the mark moves, the records are still in the log.
	if got, count, bytes := a.ack(11, 20, 10, 100); got != 0 || count != 0 || bytes != 0 {
		t.Fatalf("acking a later range gave mark %d and freed %d/%d; it must wait for the gap", got, count, bytes)
	}
	if got, count, bytes := a.ack(21, 30, 10, 100); got != 0 || count != 0 || bytes != 0 {
		t.Fatalf("mark = %d freeing %d/%d, want 0 and nothing freed while [1,10] is outstanding", got, count, bytes)
	}
	if got, count, bytes := a.ack(1, 10, 10, 100); got != 30 || count != 30 || bytes != 300 {
		t.Fatalf("closing the gap gave mark %d freeing %d records/%d bytes, want 30 and 30/300", got, count, bytes)
	}
	if got, count, bytes := a.ack(31, 40, 10, 100); got != 40 || count != 10 || bytes != 100 {
		t.Fatalf("mark = %d freeing %d/%d, want 40 and 10/100", got, count, bytes)
	}
}

func TestFlushForcesPartialBatch(t *testing.T) {
	sink := &recorder{}
	// A partial batch would otherwise wait an hour for the flush interval.
	b := openBuf(t, t.TempDir(), sink, fast(WithFlush(1000, 1<<20, time.Hour))...)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := b.Write(ctx, fmt.Sprintf("r%d", i)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush did not force the partial batch out: %v", err)
	}
	if got := sink.seen(); len(got) != 3 {
		t.Fatalf("sink saw %d records after Flush, want 3", len(got))
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestRepeatedCloseReturnsFirstError(t *testing.T) {
	sink := &recorder{}
	sink.failAll.Store(true) // nothing ever drains
	b := openBuf(t, t.TempDir(), sink, fast()...)
	if err := b.Write(context.Background(), "stuck"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	first := b.Close(ctx)
	if first == nil {
		t.Fatal("Close with a failing sink and expired context returned nil")
	}
	if again := b.Close(context.Background()); !errors.Is(again, first) {
		t.Fatalf("second Close = %v, want the first error %v", again, first)
	}
}

func TestOldestAgeReflectsDeliveryProgress(t *testing.T) {
	sink := &recorder{}
	sink.delay.Store(int64(3 * time.Millisecond)) // keep the backlog nonempty
	b := openBuf(t, t.TempDir(), sink, fast()...)
	ctx := context.Background()
	// Closed on every path: a buffer left running writes into the directory
	// while the test framework is removing it.
	defer b.Close(ctx)

	const load = 600 * time.Millisecond
	start := time.Now()
	for time.Since(start) < load {
		if err := b.Write(ctx, "x"); err != nil {
			t.Fatalf("Write: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	// The backlog was never empty, but records kept flushing, so the oldest
	// pending record is recent. Measuring "time since the backlog was last
	// empty" would report the whole run here.
	if age := b.Stats().OldestAge; age >= load {
		t.Fatalf("OldestAge = %v after %v of continuous load; it is measuring the backlog's lifetime, not the oldest record's", age, load)
	}

	// And once everything is delivered it must read as nothing pending, even
	// though writers and the flusher were racing to note and trim marks.
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if s := b.Stats(); s.PendingRecords == 0 && s.OldestAge != 0 {
		t.Fatalf("OldestAge = %v with an empty backlog: a mark outlived the record it was for", s.OldestAge)
	}
}

func TestAgeTrackerCoalescesAndTrims(t *testing.T) {
	var a ageTracker
	base := time.Now().UnixNano()
	// Far-apart marks: one entry each.
	for i := 1; i <= 4; i++ {
		a.note(uint64(i*10), base+int64(i)*ageGranularity)
	}
	now := base + 10*ageGranularity
	if got, want := a.oldest(now), time.Duration(9*ageGranularity); got != want {
		t.Fatalf("oldest = %v, want %v", got, want)
	}
	a.trim(10)
	if got, want := a.oldest(now), time.Duration(8*ageGranularity); got != want {
		t.Fatalf("oldest after trim(10) = %v, want %v", got, want)
	}
	a.trim(15) // mid-mark: the covering mark must survive
	if got, want := a.oldest(now), time.Duration(8*ageGranularity); got != want {
		t.Fatalf("oldest after trim(15) = %v, want %v", got, want)
	}
	a.trim(40)
	if got := a.oldest(now); got != 0 {
		t.Fatalf("oldest after trimming everything = %v, want 0", got)
	}

	// Overflow compacts instead of growing without bound, and never
	// understates the head's age.
	a = ageTracker{}
	for i := range ageMaxMarks * 3 {
		a.note(uint64(i+1), base+int64(i)*ageGranularity)
	}
	if len(a.marks) > ageMaxMarks {
		t.Fatalf("tracker grew to %d marks, cap is %d", len(a.marks), ageMaxMarks)
	}
	end := base + int64(ageMaxMarks*3)*ageGranularity
	if got, want := a.oldest(end), time.Duration(end-base); got != want {
		t.Fatalf("oldest after compaction = %v, want %v", got, want)
	}
}

// A sink deriving a per-record idempotency key from Batch.ID plus the record's
// index needs every batch to hold consecutive LSNs. A dropped record must
// therefore end the batch instead of leaving a hole in the numbering.
func TestBatchesHoldConsecutiveRecords(t *testing.T) {
	sink := &recorder{}
	// One batch by size: only the undecodable record should split these.
	b, err := Open[string](t.TempDir(), sink, poisonCodec{},
		WithSync(SyncNever, 0),
		WithFlush(100, 1<<20, time.Hour),
		WithCheckpointInterval(5*time.Millisecond))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	for _, rec := range []string{"a", "b", "poison", "c", "d"} {
		if err := b.Write(ctx, rec); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := b.Close(fctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	// LSNs are 1..5 with 3 undecodable, so the deliverable records must arrive
	// as [a b] starting at 1 and [c d] starting at 4.
	want := []struct {
		id      uint64
		records []string
	}{
		{1, []string{"a", "b"}},
		{4, []string{"c", "d"}},
	}
	if len(sink.batches) != len(want) {
		t.Fatalf("sink got %d batches, want %d: a dropped record must end the batch", len(sink.batches), len(want))
	}
	for i, w := range want {
		got := sink.batches[i]
		if got.ID != w.id || !slices.Equal(got.Records, w.records) {
			t.Fatalf("batch %d = {ID:%d %v}, want {ID:%d %v}", i, got.ID, got.Records, w.id, w.records)
		}
	}
}

// The floor names the oldest record still wanted. A batch's acknowledgement
// range starts below its first record whenever LSNs in front of it carried no
// record — dropped ones, or the gap recovery leaves — and a floor raised into
// that stretch must not take the batch with it.
func TestFloorSparesBatchesWhoseRecordsAreAllAboveIt(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithFlush(2, 1<<20, 10*time.Millisecond))...)
	ctx := context.Background()

	// As DropOldest would have left it: LSN 1 is shed, everything from 2 is
	// still wanted. The batch that follows holds LSNs 2 and 3, but its
	// acknowledgement range starts at 1, because somebody has to account for the
	// record that was dropped.
	b.floor.Store(2)
	for _, rec := range []string{"shed", "a", "b"} {
		if err := b.Write(ctx, rec); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	if got := sink.seen(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("sink got %v, want [a b]: a batch whose records are all at or above the floor was thrown away with the range in front of it", got)
	}
	if s := b.Stats(); s.Dropped != 1 {
		t.Fatalf("Stats.Dropped = %d, want 1 — only the record below the floor", s.Dropped)
	}
}

func TestAgeTrackerIgnoresMarksForDeliveredRecords(t *testing.T) {
	var a ageTracker
	now := time.Now().UnixNano()
	a.trim(10) // LSNs up to 10 are downstream

	// A writer that was slow to record its mark must not resurrect them.
	a.note(10, now-int64(time.Hour))
	if got := a.oldest(now); got != 0 {
		t.Fatalf("oldest = %v after a late mark for an acknowledged LSN, want 0", got)
	}
	// A genuinely newer record still registers.
	a.note(11, now)
	if got := a.oldest(now + int64(time.Second)); got != time.Second {
		t.Fatalf("oldest = %v, want 1s", got)
	}
}

// tornReopen writes a few records, closes, and chops the last segment so that
// reopening leaves the gap in the numbering that recovery makes when it cannot
// tell which LSNs already went downstream.
func tornReopen(t *testing.T, dir string) {
	t.Helper()
	sink := &recorder{}
	b := openBuf(t, dir, sink, fast(WithSync(SyncAlways, 0))...)
	ctx := context.Background()
	for i := range 5 {
		if err := b.Write(ctx, fmt.Sprintf("old-%d", i)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	closeBuf(t, b)

	segs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments in %s: %v", dir, err)
	}
	slices.Sort(segs)
	last := segs[len(segs)-1]
	st, err := os.Stat(last)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(last, st.Size()/2); err != nil {
		t.Fatal(err)
	}
}

// DropOldest has to keep freeing capacity after a recovery gap. The records it
// shed are counted off from the acknowledged mark, and if that mark stays parked
// below a stretch that holds nothing, the policy sheds nothing at all and every
// writer waits for a sink that is never coming back.
func TestDropOldestKeepsWorkingAcrossARecoveryGap(t *testing.T) {
	dir := t.TempDir()
	tornReopen(t, dir)

	dead := &recorder{}
	dead.failAll.Store(true)
	b := openBuf(t, dir, dead, fast(
		WithCapacity(8, 0),
		WithOnFull(DropOldest()),
		WithRetry(Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	// Well past the capacity, against a sink that never accepts anything.
	// DropOldest must never park a writer.
	for i := range 40 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := b.Write(ctx, fmt.Sprintf("new-%d", i))
		cancel()
		if err != nil {
			t.Fatalf("write %d: %v; DropOldest stopped freeing capacity after the recovery gap", i, err)
		}
	}
}

// The other half of the same behaviour, stated directly: the mark moves past a
// stretch that carries no record without waiting for a delivery, because
// nothing in that stretch can ever be delivered.
func TestRecoveryGapIsAcknowledgedWithoutDelivery(t *testing.T) {
	dir := t.TempDir()
	tornReopen(t, dir)

	dead := &recorder{}
	dead.failAll.Store(true)
	b := openBuf(t, dir, dead, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	before := b.Stats().Checkpoint
	if err := b.Write(context.Background(), "after the gap"); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Nothing is ever delivered, so any progress in the mark is the empty
	// stretch being accounted for.
	waitFor(t, 5*time.Second, func() bool { return b.Stats().Checkpoint > before })
	if s := b.Stats(); s.Flushed != 0 {
		t.Fatalf("Stats.Flushed = %d, want 0: the sink never accepted anything", s.Flushed)
	}
}

// Opening a buffer after a crash and closing it again without writing leaves a
// recovery segment that holds nothing. Flush must not then wait for records
// that were never written.
func TestFlushReturnsAfterAnUnusedRecoveryReopen(t *testing.T) {
	dir := t.TempDir()
	tornReopen(t, dir)

	closeBuf(t, openBuf(t, dir, &recorder{}, fast()...)) // opens the gap, writes nothing

	sink := &recorder{}
	b := openBuf(t, dir, sink, fast()...)
	defer closeBuf(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v; it is waiting for records the recovery segment never held", err)
	}
}

// dlqRecorder counts every attempt the dead-letter sink is given, which is what
// separates "retried and eventually landed" from "took it first time".
type dlqRecorder struct {
	recorder
	attempts atomic.Int64

	amu   sync.Mutex
	perID map[uint64]int
}

func (d *dlqRecorder) Flush(ctx context.Context, b Batch[string]) error {
	d.attempts.Add(1)
	d.amu.Lock()
	if d.perID == nil {
		d.perID = map[uint64]int{}
	}
	d.perID[b.ID]++
	d.amu.Unlock()
	return d.recorder.Flush(ctx, b)
}

// attemptsPerBatch reports how many times each batch was offered. The flusher
// is free to cut batches wherever it likes, so a total attempt count says
// nothing on its own; the per-batch count is the configured limit.
func (d *dlqRecorder) attemptsPerBatch() map[uint64]int {
	d.amu.Lock()
	defer d.amu.Unlock()
	return maps.Clone(d.perID)
}

func TestDeadLetterRetriesBeforeDropping(t *testing.T) {
	// A dead-letter sink that blips must not cost the records. Before the
	// dead-letter sink was retried, the first error discarded the whole batch.
	sink := &recorder{}
	sink.failAll.Store(true)
	dlq := &dlqRecorder{}
	dlq.failures.Store(2) // two blips, then it accepts

	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 1),
		WithDeadLetter[string](dlq),
		WithDeadLetterRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 5),
	)...)
	ctx := context.Background()
	want := []string{"e0", "e1", "e2"}
	for _, v := range want {
		if err := b.Write(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	if got := dlq.seen(); !slices.Equal(got, want) {
		t.Fatalf("dead-letter sink saw %q, want %q: a transient dead-letter failure must not lose the batch", got, want)
	}
	if got := dlq.attempts.Load(); got != 3 {
		t.Fatalf("dead-letter sink was called %d times, want 3 (two failures then success)", got)
	}
	if s := b.Stats(); s.DeadLettered != 3 || s.Dropped != 0 {
		t.Fatalf("Stats reports %d dead-lettered and %d dropped, want 3 and 0", s.DeadLettered, s.Dropped)
	}
}

func TestDeadLetterExhaustsAttemptsAndDrops(t *testing.T) {
	// Bounded is the other half of the contract: a dead-letter sink that is
	// also down must not wedge the pipeline behind it.
	sink := &recorder{}
	sink.failAll.Store(true)
	dlq := &dlqRecorder{}
	dlq.failAll.Store(true)

	c := newCounters()
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 1),
		WithDeadLetter[string](dlq),
		WithDeadLetterRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 2),
		WithObserver(c.observer()),
	)...)
	ctx := context.Background()
	for i := range 4 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: an exhausted dead-letter sink must still advance the checkpoint: %v", err)
	}
	closeBuf(t, b)

	for id, n := range dlq.attemptsPerBatch() {
		if n != 2 {
			t.Fatalf("dead-letter sink was offered batch %d %d times, want exactly the 2 attempts configured", id, n)
		}
	}
	if s := b.Stats(); s.Dropped != 4 || s.DeadLettered != 0 {
		t.Fatalf("Stats reports %d dropped and %d dead-lettered, want 4 and 0", s.Dropped, s.DeadLettered)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.drops[ReasonRetriesExhausted] != 4 {
		t.Fatalf("ReasonRetriesExhausted drops = %d, want 4", c.drops[ReasonRetriesExhausted])
	}
	dlqFlushes := 0
	for _, f := range c.flushes {
		if f.DeadLetter {
			dlqFlushes++
		}
	}
	if want := int(dlq.attempts.Load()); dlqFlushes != want {
		t.Fatalf("OnFlush reported %d dead-letter attempts, want %d: a dead-letter sink stuck retrying must be visible", dlqFlushes, want)
	}
}

func TestDeadLetterRetryAbandonedByDropOldest(t *testing.T) {
	// A batch waiting out its dead-letter backoff holds capacity, and only
	// abandoning it whole gives that capacity back. Without the floor check in
	// the dead-letter loop, DropOldest — the one policy that exists never to
	// block — would block here.
	sink := &recorder{}
	sink.failAll.Store(true)
	dlq := &dlqRecorder{}
	dlq.failAll.Store(true)

	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(2, 0),
		WithOnFull(DropOldest()),
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 1),
		WithDeadLetter[string](dlq),
		// Long enough that the write below can only get through by abandoning
		// the batch, not by outwaiting the backoff.
		WithDeadLetterRetry(Backoff{Initial: time.Hour, Jitter: 0}, 0),
	)...)
	// A batch parked in an hour-long backoff cannot drain, so Close gives up
	// waiting rather than returning cleanly. That is the point of the setup.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		b.Close(ctx)
	}()

	ctx := context.Background()
	for i := range 2 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, func() bool { return dlq.attempts.Load() > 0 })

	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Write(wctx, "e2"); err != nil {
		t.Fatalf("Write under DropOldest: %v; capacity held by a batch parked in dead-letter backoff must be reclaimable", err)
	}
}

func TestDecodeFailureHandlerSeesPayload(t *testing.T) {
	// Quarantine is the caller's to implement; the buffer's job is to hand over
	// the LSN and the stored bytes.
	dir := t.TempDir()
	sink := &recorder{}

	type seen struct {
		lsn     uint64
		payload string
	}
	var mu sync.Mutex
	var got []seen

	b, err := Open[string](dir, sink, poisonCodec{}, fast(
		WithOnDecodeFailure(func(f DecodeFailure) DecodeAction {
			mu.Lock()
			defer mu.Unlock()
			// Payload aliases the read buffer, so it is copied here exactly as
			// the doc comment tells a caller to.
			got = append(got, seen{lsn: f.LSN, payload: string(f.Payload)})
			return DropRecord
		}),
	)...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	ctx := context.Background()
	for _, v := range []string{"a", "poison", "b"} {
		if err := b.Write(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("handler ran %d times, want 1", len(got))
	}
	if got[0].payload != "poison" {
		t.Fatalf("handler saw payload %q, want %q", got[0].payload, "poison")
	}
	if got[0].lsn == 0 {
		t.Fatal("handler saw LSN 0, want the record's own sequence number")
	}
	if s := b.Stats(); s.Dropped != 1 {
		t.Fatalf("Stats.Dropped = %d, want 1: DropRecord must keep the behaviour it replaced", s.Dropped)
	}
	if want := []string{"a", "b"}; !slices.Equal(sink.seen(), want) {
		t.Fatalf("sink saw %q, want %q: DropRecord must not disturb the records around it", sink.seen(), want)
	}
}

func TestDecodeFailureStopsAndReplays(t *testing.T) {
	// The scenario the option exists for: a codec that changed between runs
	// meets a backlog the previous build wrote. Stopping must keep every record
	// from the poisoned one on, so a restart with a codec that understands them
	// still delivers them. Nothing may go missing across the three opens.
	dir := t.TempDir()
	want := []string{"a", "b", "poison", "c", "d"}
	ctx := context.Background()

	// Round one: a codec that has no trouble with them writes them down, and a
	// sink that is down from the outset leaves the whole lot in the backlog.
	first := &recorder{}
	first.failAll.Store(true)
	// Retries stay unbounded (the default) so the batch is never given up on
	// and dropped: the point is to leave it in the log for the next open.
	b := openBuf(t, dir, first, fast()...)
	for _, v := range want {
		if err := b.Write(ctx, v); err != nil {
			t.Fatal(err)
		}
	}
	cctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if err := b.Close(cctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close against a failing sink = %v, want its deadline", err)
	}

	// Round two: the new build's codec cannot read what the old one wrote. The
	// records ahead of the poisoned one still go, then delivery stops.
	var quarantined atomic.Value
	second := &recorder{}
	b2, err := Open[string](dir, second, poisonCodec{}, fast(
		WithOnDecodeFailure(func(f DecodeFailure) DecodeAction {
			quarantined.Store(string(f.Payload))
			return StopBuffer
		}),
	)...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool { return b2.Stats().Err != nil })
	if got, _ := quarantined.Load().(string); got != "poison" {
		t.Fatalf("handler was given %q, want %q", got, "poison")
	}
	if werr := b2.Write(ctx, "late"); !errors.Is(werr, ErrFailed) {
		t.Fatalf("Write on a stopped buffer = %v, want ErrFailed: a buffer that will never drain must say so", werr)
	}
	// Close reports the failure rather than pretending the buffer drained.
	cctx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	if err := b2.Close(cctx2); err == nil {
		t.Fatal("Close on a stopped buffer returned nil, want the decode failure that stopped it")
	}

	// Round three: the codec understands them again. Everything the stopped
	// buffer held back must arrive, poisoned record first.
	third := &recorder{}
	b3 := openBuf(t, dir, third, fast()...)
	defer closeBuf(t, b3)
	rest := want[len(second.seen()):]
	waitFor(t, 5*time.Second, func() bool { return len(third.seen()) == len(rest) })

	if got := third.seen(); !slices.Equal(got, rest) {
		t.Fatalf("after the codec was fixed the sink saw %q, want %q: StopBuffer must keep every record from the poisoned one on", got, rest)
	}
	if !slices.Contains(third.seen(), "poison") {
		t.Fatal("the record that stopped the buffer was not redelivered; StopBuffer exists to keep it")
	}
	if got := slices.Concat(second.seen(), third.seen()); !slices.Equal(got, want) {
		t.Fatalf("across the three opens the sinks saw %q, want %q: no record may be lost to a codec change", got, want)
	}
}

func TestOversizedWriteFailsInsteadOfBlocking(t *testing.T) {
	// The bug this guards against is a write that never returns: capacity is
	// measured against the whole backlog, so a batch above it fails to reserve
	// even against an empty buffer, and Block waits for space that cannot exist.
	for _, tc := range []struct {
		name string
		cap  Option
		vs   []string
	}{
		{"records", WithCapacity(5, 0), make([]string, 10)},
		{"bytes", WithCapacity(0, 8), []string{"a record well past eight bytes"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := openBuf(t, t.TempDir(), &recorder{}, fast(tc.cap)...)
			defer closeBuf(t, b)

			// Generous next to the write, tight next to blocking for ever: if the
			// deadline is what ends the call, the check below says so.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := b.WriteBatch(ctx, tc.vs...)
			if !errors.Is(err, ErrTooLarge) {
				t.Fatalf("WriteBatch = %v, want ErrTooLarge", err)
			}
			if ctx.Err() != nil {
				t.Fatal("the context expired, so the write blocked rather than failing on its own")
			}
		})
	}
}

func TestOversizedWriteFailsUnderEveryPolicy(t *testing.T) {
	// No policy can make an impossible write possible. DropNewest is the one
	// that matters most: reporting success for a record that was never going to
	// be admitted would lose it silently.
	for name, p := range map[string]Policy{
		"block":       Block(),
		"reject":      Reject(),
		"drop_newest": DropNewest(),
		"drop_oldest": DropOldest(),
	} {
		t.Run(name, func(t *testing.T) {
			b := openBuf(t, t.TempDir(), &recorder{}, fast(
				WithCapacity(5, 0),
				WithOnFull(p),
			)...)
			defer closeBuf(t, b)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := b.WriteBatch(ctx, make([]string, 10)...); !errors.Is(err, ErrTooLarge) {
				t.Fatalf("WriteBatch = %v, want ErrTooLarge", err)
			}
			if s := b.Stats(); s.Written != 0 {
				t.Fatalf("Written = %d, want 0; the write was refused, so nothing was accepted", s.Written)
			}
		})
	}
}

func TestUnlimitedCapacityAdmitsAnyWrite(t *testing.T) {
	// Zero means unlimited, so the size check must not fire on it.
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithCapacity(0, 0))...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	vs := make([]string, 500)
	for i := range vs {
		vs[i] = fmt.Sprintf("e%d", i)
	}
	if err := b.WriteBatch(ctx, vs...); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
}

func TestPolicySeesWaitedGrowing(t *testing.T) {
	// Waited has to measure this write, so it starts near zero however stale the
	// backlog already is, and grows as the write is reconsidered.
	sink := &recorder{}
	sink.failAll.Store(true)

	var mu sync.Mutex
	var waits []time.Duration
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(1, 0),
		WithRetry(Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond, Jitter: 0}, 0),
		WithOnFull(PolicyFunc(func(_ context.Context, s State) Decision {
			mu.Lock()
			waits = append(waits, s.Waited)
			n := len(waits)
			mu.Unlock()
			if n >= 5 {
				return DecisionReject // terminate rather than block for ever
			}
			return DecisionBlock
		})),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 10 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); errors.Is(err, ErrFull) {
			break
		} else if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(waits) < 2 {
		t.Fatalf("the policy was consulted %d times, too few to say anything about Waited", len(waits))
	}
	if waits[0] > 50*time.Millisecond {
		t.Fatalf("first Waited = %v, want near zero: it measures this write, not the backlog", waits[0])
	}
	if waits[len(waits)-1] <= waits[0] {
		t.Fatalf("Waited did not grow across reconsiderations: %v", waits)
	}
}

func TestWaitedDrivesAPerWriteDeadline(t *testing.T) {
	// The capability the age-based BlockThenDropOldest deliberately does not
	// offer: a deadline belonging to the write rather than to the backlog.
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithCapacity(5, 0),
		WithRetry(Backoff{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond, Jitter: 0}, 0),
		WithOnFull(PolicyFunc(func(_ context.Context, s State) Decision {
			if s.Waited > 50*time.Millisecond {
				return DecisionDropOldest
			}
			return DecisionBlock
		})),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 30 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v; the deadline passed, so the policy must shed instead of blocking on", i, err)
		}
	}
	if s := b.Stats(); s.Dropped == 0 {
		t.Fatal("nothing was dropped, so the writes cannot have gone through the shedding path")
	}
}

func TestFlushReturnsWhenRecordsWereDropped(t *testing.T) {
	// Flush waits for records to be resolved, not delivered. A batch that ran
	// out of attempts with no dead-letter sink is resolved: it will never reach
	// the sink, and Flush must not wait for something that cannot happen.
	sink := &recorder{}
	sink.failAll.Store(true)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0}, 1),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	fctx, fcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fcancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v; the records were dropped rather than delivered, and Flush waits for records to be resolved", err)
	}
	s := b.Stats()
	if s.Flushed != 0 {
		t.Fatalf("Flushed = %d, want 0: the sink failed every attempt", s.Flushed)
	}
	if s.Dropped == 0 {
		t.Fatal("nothing was dropped, so this does not exercise the resolved-but-not-delivered path")
	}
	if s.PendingRecords != 0 {
		t.Fatalf("PendingRecords = %d, want 0: Flush returned, so nothing written before it is still pending", s.PendingRecords)
	}
}

// errPermanentSink is a stand-in for a destination that has given a verdict:
// the batch is malformed and asking again will not change that.
var errRejected = fmt.Errorf("row exceeds column width: %w", ErrPermanent)

func TestPermanentErrorSkipsRemainingAttempts(t *testing.T) {
	// A verdict is a verdict on the first attempt. Spending the other nine to
	// collect it again is the "consuming all attempts unnecessarily" half.
	sink := &recorder{}
	sink.fail(errRejected)
	dlq := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: 50 * time.Millisecond, Max: time.Second, Jitter: 0}, 10),
		WithDeadLetter[string](dlq),
	)...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Write(ctx, "e0"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if n := sink.calls(); n != 1 {
		t.Fatalf("primary sink was called %d times, want 1: ErrPermanent means stop asking", n)
	}
	if got := dlq.seen(); len(got) != 1 || got[0] != "e0" {
		t.Fatalf("dead-letter sink saw %v, want [e0]", got)
	}
	if s := b.Stats(); s.DeadLettered != 1 {
		t.Fatalf("DeadLettered = %d, want 1", s.DeadLettered)
	}
}

func TestPermanentErrorDoesNotWedgeThePipeline(t *testing.T) {
	// The headline case. Under stock settings a rejected batch retries forever,
	// holding the only in-flight slot and pinning the low-water mark, so the
	// whole pipeline stops behind it. Classification is what lets the buffer
	// step over it.
	var rejectFirst atomic.Bool
	rejectFirst.Store(true)
	var delivered []string
	var mu sync.Mutex
	sink := SinkFunc[string](func(_ context.Context, batch Batch[string]) error {
		if batch.ID == 1 && rejectFirst.Load() {
			return errRejected
		}
		mu.Lock()
		defer mu.Unlock()
		delivered = append(delivered, batch.Records...)
		return nil
	})

	// Deliberately stock: retry forever, one delivery at a time. These are the
	// defaults that turn one bad batch into a stalled application.
	b := openBuf(t, t.TempDir(), sink,
		WithSync(SyncNever, 0),
		WithFlush(1, 0, 5*time.Millisecond),
		WithCheckpointInterval(5*time.Millisecond),
		WithCapacity(4, 0),
	)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v; the rejected batch is wedging the pipeline", i, err)
		}
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	mu.Lock()
	got := len(delivered)
	mu.Unlock()
	if got < 19 {
		t.Fatalf("delivered %d records, want the 19 that follow the rejected batch", got)
	}
	if s := b.Stats(); s.Dropped != 1 {
		t.Fatalf("Dropped = %d, want 1: the rejected batch had nowhere to go", s.Dropped)
	}
}

func TestFailBufferKeepsRecordsForReplay(t *testing.T) {
	// The fault is the destination, not the batch, so nothing may be disposed
	// of: a restart with working credentials has to find every record.
	dir := t.TempDir()
	sink := &recorder{}
	sink.fail(errors.New("401 unauthorized"))
	dlq := &recorder{}
	b := openBuf(t, dir, sink, fast(
		WithDeadLetter[string](dlq),
		WithOnSinkError(func(SinkFailure) RetryDecision {
			return RetryDecision{Action: FailBuffer}
		}),
	)...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 5 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// The failure has to reach a writer rather than only Stats.
	deadline := time.Now().Add(2 * time.Second)
	var writeErr error
	for time.Now().Before(deadline) {
		if writeErr = b.Write(ctx, "after"); writeErr != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(writeErr, ErrFailed) {
		t.Fatalf("Write after FailBuffer = %v, want ErrFailed", writeErr)
	}
	if s := b.Stats(); s.Err == nil {
		t.Fatal("Stats().Err is nil, so nothing records why the buffer stopped")
	} else if s.DeadLettered != 0 || s.Dropped != 0 {
		t.Fatalf("DeadLettered=%d Dropped=%d, want 0 and 0: FailBuffer disposes of nothing", s.DeadLettered, s.Dropped)
	}
	if got := dlq.seen(); len(got) != 0 {
		t.Fatalf("dead-letter sink saw %v, want nothing", got)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	_ = b.Close(cctx) // returns the fatal error by design

	// Reopen with a healthy sink: everything written must still be there.
	healthy := &recorder{}
	b2 := openBuf(t, dir, healthy, fast()...)
	defer closeBuf(t, b2)
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(healthy.seen()) >= 5 {
			break
		}
		_ = b2.Flush(rctx)
		time.Sleep(10 * time.Millisecond)
	}
	if got := healthy.seen(); len(got) < 5 {
		t.Fatalf("after reopen the sink saw %v, want the 5 records FailBuffer kept", got)
	}
}

func TestRetryAfterOverridesTheBackoff(t *testing.T) {
	// The ladder is deliberately far away from the override in both directions,
	// so neither result can be produced by the ladder alone.
	for _, tc := range []struct {
		name     string
		override time.Duration
		ladder   Backoff
		min, max time.Duration
	}{
		// Ladder far shorter than the override: waiting longer proves the
		// override was used.
		{"slower than the ladder", 200 * time.Millisecond,
			Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0},
			150 * time.Millisecond, 2 * time.Second},
		// Ladder far longer: returning sooner proves the same.
		{"faster than the ladder", 20 * time.Millisecond,
			Backoff{Initial: 5 * time.Second, Max: 10 * time.Second, Jitter: 0},
			10 * time.Millisecond, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &recorder{}
			sink.failures.Store(2) // fail twice, then accept
			sink.failWith.Store(func() *error {
				e := RetryAfter(tc.override, errors.New("throttled"))
				return &e
			}())
			b := openBuf(t, t.TempDir(), sink, fast(WithRetry(tc.ladder, 0))...)
			defer closeBuf(t, b)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := b.Write(ctx, "e0"); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := b.Flush(ctx); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			gaps := sink.gaps()
			if len(gaps) < 2 {
				t.Fatalf("only %d gaps between attempts, want 2", len(gaps))
			}
			for i, g := range gaps[:2] {
				if g < tc.min || g > tc.max {
					t.Fatalf("gap %d was %v, want between %v and %v: the sink's delay must replace the ladder", i, g, tc.min, tc.max)
				}
			}
		})
	}
}

func TestClassifierDoesNotRunOnShutdownCancellation(t *testing.T) {
	// Close's deadline cancels the delivery context and the sink returns that
	// like any other error. A handler shown it could reasonably call the batch
	// permanent — so the classification must not be acted on during shutdown,
	// or records are dead-lettered by a timeout rather than by a verdict.
	dir := t.TempDir()
	sink := &recorder{}
	sink.delay.Store(int64(time.Hour)) // blocks until its context is cancelled
	dlq := &recorder{}
	b := openBuf(t, dir, sink, fast(
		WithDeadLetter[string](dlq),
		WithOnSinkError(func(SinkFailure) RetryDecision {
			return RetryDecision{Action: DeadLetterBatch}
		}),
	)...)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 3 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the batch reach the sink

	cctx, ccancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer ccancel()
	_ = b.Close(cctx) // times out by design

	if s := b.Stats(); s.DeadLettered != 0 || s.Dropped != 0 {
		t.Fatalf("DeadLettered=%d Dropped=%d, want 0 and 0: the shutdown cancelled the sink, it did not reject the batch", s.DeadLettered, s.Dropped)
	}
	if got := dlq.seen(); len(got) != 0 {
		t.Fatalf("dead-letter sink saw %v during a shutdown, want nothing", got)
	}

	// And the records survived to be delivered by the next run.
	healthy := &recorder{}
	b2 := openBuf(t, dir, healthy, fast()...)
	defer closeBuf(t, b2)
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(healthy.seen()) >= 3 {
			break
		}
		_ = b2.Flush(rctx)
		time.Sleep(10 * time.Millisecond)
	}
	if got := healthy.seen(); len(got) < 3 {
		t.Fatalf("after reopen the sink saw %v, want the 3 records the shutdown kept", got)
	}
}

func TestDeadLetterSinkClassification(t *testing.T) {
	// A dead-letter sink that rejects permanently has nowhere further to send
	// the batch, so the verdict means give up now rather than work through the
	// three default attempts to the same place.
	sink := &recorder{}
	sink.fail(errSink)
	dlq := &recorder{}
	dlq.fail(errRejected)

	var sawDeadLetter atomic.Bool
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Jitter: 0}, 1),
		WithDeadLetter[string](dlq),
		WithOnSinkError(func(f SinkFailure) RetryDecision {
			if f.DeadLetter {
				sawDeadLetter.Store(true)
				return RetryDecision{Action: DeadLetterBatch}
			}
			return RetryDecision{} // the primary sink retries normally
		}),
	)...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Write(ctx, "e0"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !sawDeadLetter.Load() {
		t.Fatal("the handler never saw SinkFailure.DeadLetter, so it cannot answer the two sinks differently")
	}
	if n := dlq.calls(); n != 1 {
		t.Fatalf("dead-letter sink was called %d times, want 1 rather than the default 3", n)
	}
	if s := b.Stats(); s.Dropped != 1 || s.DeadLettered != 0 {
		t.Fatalf("Dropped=%d DeadLettered=%d, want 1 and 0", s.Dropped, s.DeadLettered)
	}
}

func TestNoClassifierIsUnchanged(t *testing.T) {
	// The compatibility guard: a plain error with no handler and no sentinel
	// retries on the ladder exactly as it always did.
	sink := &recorder{}
	sink.failures.Store(3)
	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}, 0),
	)...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Write(ctx, "e0"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := sink.seen(); len(got) != 1 || got[0] != "e0" {
		t.Fatalf("sink saw %v, want [e0] after retrying through the failures", got)
	}
	if s := b.Stats(); s.Dropped != 0 || s.DeadLettered != 0 {
		t.Fatalf("Dropped=%d DeadLettered=%d, want 0 and 0: a plain error is transient", s.Dropped, s.DeadLettered)
	}
}

func TestObserverReportsRetryAction(t *testing.T) {
	// A rejection and an outage must not be the same increment on the same
	// counter: they want opposite responses from whoever is paged.
	sink := &recorder{}
	sink.fail(errRejected)
	var mu sync.Mutex
	var actions []RetryAction
	b := openBuf(t, t.TempDir(), sink, fast(
		WithObserver(Observer{OnFlush: func(f FlushInfo) {
			if f.Err != nil {
				mu.Lock()
				actions = append(actions, f.Action)
				mu.Unlock()
			}
		}}),
	)...)
	defer closeBuf(t, b)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Write(ctx, "e0"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(actions) != 1 || actions[0] != DeadLetterBatch {
		t.Fatalf("OnFlush reported actions %v, want one DeadLetterBatch", actions)
	}
	if DeadLetterBatch.String() != "dead_letter" {
		t.Fatalf("RetryAction.String() = %q, want a usable metric label", DeadLetterBatch.String())
	}
}
