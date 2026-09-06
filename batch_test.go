package batch

import (
	"context"
	"errors"
	"fmt"
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
