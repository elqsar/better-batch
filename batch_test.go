package batch

import (
	"context"
	"errors"
	"fmt"
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
	failures atomic.Int64 // fail this many more flushes
	failAll  atomic.Bool
	delay    atomic.Int64 // nanoseconds to sleep in Flush
}

var errSink = errors.New("sink is down")

func (r *recorder) Flush(ctx context.Context, b Batch[string]) error {
	if d := r.delay.Load(); d > 0 {
		select {
		case <-time.After(time.Duration(d)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.failAll.Load() || r.failures.Add(-1) >= 0 {
		return errSink
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, b)
	r.records = append(r.records, b.Records...)
	return nil
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

func TestAckTrackerLowWaterMark(t *testing.T) {
	a := newAckTracker(0)

	if got := a.ack(11, 20); got != 0 {
		t.Fatalf("acking a later range moved the mark to %d; it must wait for the gap", got)
	}
	if got := a.ack(21, 30); got != 0 {
		t.Fatalf("mark = %d, want 0 while [1,10] is outstanding", got)
	}
	if got := a.ack(1, 10); got != 30 {
		t.Fatalf("closing the gap gave mark %d, want 30", got)
	}
	if got := a.ack(31, 40); got != 40 {
		t.Fatalf("mark = %d, want 40", got)
	}
}
