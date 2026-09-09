package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// poisonCodec fails to decode one specific payload, to exercise ReasonDecode.
type poisonCodec struct{}

func (poisonCodec) Encode(dst []byte, v string) ([]byte, error) { return append(dst, v...), nil }
func (poisonCodec) Decode(src []byte) (string, error) {
	if string(src) == "poison" {
		return "", errors.New("undecodable")
	}
	return string(src), nil
}

// counters collects everything an Observer reports.
type counters struct {
	mu           sync.Mutex
	writes       int
	writeRecords int
	writeBytes   int
	flushes      []FlushInfo
	drops        map[DropReason]int
	deadLettered int
	decisions    map[Decision]int
	diskFullSeen bool
	checkpoints  []uint64
}

func newCounters() *counters {
	return &counters{drops: map[DropReason]int{}, decisions: map[Decision]int{}}
}

func (c *counters) observer() Observer {
	return Observer{
		OnWrite: func(records, bytes int) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.writes++
			c.writeRecords += records
			c.writeBytes += bytes
		},
		OnFlush: func(f FlushInfo) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.flushes = append(c.flushes, f)
		},
		OnDrop: func(records int, reason DropReason) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.drops[reason] += records
		},
		OnDeadLetter: func(records int) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.deadLettered += records
		},
		OnBackpressure: func(s State, d Decision) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.decisions[d]++
			if s.DiskFull {
				c.diskFullSeen = true
			}
		},
		OnCheckpoint: func(lsn uint64) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.checkpoints = append(c.checkpoints, lsn)
		},
	}
}

func (c *counters) snapshot(fn func(*counters)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn(c)
}

func TestObserverReportsWritesAndFlushes(t *testing.T) {
	c := newCounters()
	sink := &recorder{}
	sink.delay.Store(int64(2 * time.Millisecond))

	// Size-triggered only: with an interval short enough to fire mid-fill, the
	// flusher legitimately dispatches partial batches and the count is timing
	// dependent. Close drains whatever is left over.
	b := openBuf(t, t.TempDir(), sink, fast(
		WithFlush(10, 0, time.Hour),
		WithObserver(c.observer()),
	)...)
	ctx := context.Background()

	for i := range 50 {
		if err := b.Write(ctx, fmt.Sprintf("event-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	c.snapshot(func(c *counters) {
		if c.writes != 50 || c.writeRecords != 50 {
			t.Fatalf("OnWrite fired %d times for %d records, want 50 and 50", c.writes, c.writeRecords)
		}
		if c.writeBytes != 50*len("event-00") {
			t.Fatalf("OnWrite reported %d bytes, want %d", c.writeBytes, 50*len("event-00"))
		}
		if len(c.flushes) != 5 {
			t.Fatalf("OnFlush fired %d times, want 5 batches of 10", len(c.flushes))
		}
		total := 0
		for _, f := range c.flushes {
			if f.Err != nil {
				t.Fatalf("batch %d reported %v", f.ID, f.Err)
			}
			if f.Attempt != 1 {
				t.Fatalf("batch %d reported attempt %d, want 1", f.ID, f.Attempt)
			}
			if f.Duration <= 0 {
				t.Fatalf("batch %d reported duration %v; a latency histogram needs a real value", f.ID, f.Duration)
			}
			total += f.Records
		}
		if total != 50 {
			t.Fatalf("flushes covered %d records, want 50", total)
		}
		if len(c.checkpoints) == 0 {
			t.Fatal("OnCheckpoint never fired even though everything was delivered")
		}
	})
}

func TestObserverReportsFailedAttempts(t *testing.T) {
	c := newCounters()
	sink := &recorder{}
	sink.failures.Store(3)

	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 0),
		WithObserver(c.observer()),
	)...)
	ctx := context.Background()
	if err := b.Write(ctx, "one"); err != nil {
		t.Fatal(err)
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	c.snapshot(func(c *counters) {
		var failed, ok int
		for _, f := range c.flushes {
			if f.Err != nil {
				failed++
			} else {
				ok++
			}
		}
		if failed == 0 || ok != 1 {
			t.Fatalf("saw %d failed and %d successful attempts, want several failures then one success", failed, ok)
		}
		// Attempts must be numbered so a histogram can bucket retries.
		for i, f := range c.flushes {
			if f.Attempt != i+1 {
				t.Fatalf("attempt %d reported Attempt=%d", i, f.Attempt)
			}
		}
	})
}

func TestObserverReportsDropReasons(t *testing.T) {
	t.Run("policy", func(t *testing.T) {
		c := newCounters()
		sink := &recorder{}
		sink.failAll.Store(true)
		b, err := Open[string](t.TempDir(), sink, stringCodec{}, fast(
			WithCapacity(5, 0),
			WithOnFull(DropNewest()),
			WithRetry(Backoff{Initial: time.Second, Jitter: 0}, 0),
			WithObserver(c.observer()),
		)...)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		for i := range 60 {
			if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
				t.Fatal(err)
			}
		}
		cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_ = b.Close(cctx)

		c.snapshot(func(c *counters) {
			if c.drops[ReasonPolicy] == 0 {
				t.Fatal("no ReasonPolicy drops reported")
			}
			if c.decisions[DecisionDropNewest] == 0 {
				t.Fatal("OnBackpressure never reported DecisionDropNewest")
			}
		})
	})

	t.Run("decode", func(t *testing.T) {
		c := newCounters()
		sink := &recorder{}
		b, err := Open[string](t.TempDir(), sink, poisonCodec{}, fast(
			WithObserver(c.observer()),
		)...)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		for _, rec := range []string{"good-1", "poison", "good-2"} {
			if err := b.Write(ctx, rec); err != nil {
				t.Fatal(err)
			}
		}
		fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := b.Flush(fctx); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(fctx); err != nil {
			t.Fatal(err)
		}

		c.snapshot(func(c *counters) {
			if c.drops[ReasonDecode] != 1 {
				t.Fatalf("ReasonDecode drops = %d, want 1", c.drops[ReasonDecode])
			}
		})
		if got := len(sink.seen()); got != 2 {
			t.Fatalf("sink got %d records, want the 2 that decoded; a poison record must not block the rest", got)
		}
	})

	t.Run("retries exhausted", func(t *testing.T) {
		c := newCounters()
		sink := &recorder{}
		sink.failAll.Store(true)
		b, err := Open[string](t.TempDir(), sink, stringCodec{}, fast(
			WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 2),
			WithObserver(c.observer()),
		)...)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		if err := b.Write(ctx, "doomed"); err != nil {
			t.Fatal(err)
		}
		fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := b.Flush(fctx); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(fctx); err != nil {
			t.Fatal(err)
		}

		c.snapshot(func(c *counters) {
			if c.drops[ReasonRetriesExhausted] != 1 {
				t.Fatalf("ReasonRetriesExhausted drops = %d, want 1", c.drops[ReasonRetriesExhausted])
			}
		})
	})
}

func TestObserverReportsDeadLetterAndDiskFull(t *testing.T) {
	c := newCounters()
	sink := &recorder{}
	sink.failAll.Store(true)
	dlq := &recorder{}

	b := openBuf(t, t.TempDir(), sink, fast(
		WithRetry(Backoff{Initial: time.Millisecond, Jitter: 0}, 2),
		WithDeadLetter[string](dlq),
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
		t.Fatal(err)
	}
	closeBuf(t, b)

	c.snapshot(func(c *counters) {
		if c.deadLettered != 4 {
			t.Fatalf("OnDeadLetter reported %d records, want 4", c.deadLettered)
		}
	})

	// A full disk must be distinguishable from a full buffer.
	c2 := newCounters()
	var failing atomic.Bool
	failing.Store(true)
	b2 := openBuf(t, t.TempDir(), &recorder{}, fast(
		WithOnFull(Reject()),
		WithObserver(c2.observer()),
		withWriteFault(enospcAfter(&failing)),
	)...)
	if err := b2.Write(ctx, "no room"); !errors.Is(err, ErrDiskFull) {
		t.Fatalf("Write = %v, want ErrDiskFull", err)
	}
	cctx, ccancel := context.WithTimeout(ctx, time.Second)
	defer ccancel()
	_ = b2.Close(cctx)

	c2.snapshot(func(c *counters) {
		if !c.diskFullSeen {
			t.Fatal("OnBackpressure never reported State.DiskFull; a full disk is indistinguishable from a full buffer")
		}
	})
}

func TestPartialObserverIsSafe(t *testing.T) {
	var writes atomic.Int64
	// Only one hook is set; every other call site must skip its nil field.
	b := openBuf(t, t.TempDir(), &recorder{}, fast(
		WithObserver(Observer{
			OnWrite: func(records, _ int) { writes.Add(int64(records)) },
		}),
	)...)
	ctx := context.Background()
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	if writes.Load() != 20 {
		t.Fatalf("OnWrite saw %d records, want 20", writes.Load())
	}
}

// TestStatsIsSafeFromCallbacks pins the one re-entrant call a synchronous
// callback is allowed to make. Stats touches only atomics and mutexes the
// callbacks are never invoked under, so every hook may read it; Write, Flush and
// Close may not, which is what WithAsyncObserver exists for.
func TestStatsIsSafeFromCallbacks(t *testing.T) {
	sink := &recorder{}
	var seen atomic.Int64
	read := func(b **Buffer[string]) func() {
		return func() {
			if *b != nil {
				_ = (*b).Stats()
				seen.Add(1)
			}
		}
	}
	var b *Buffer[string]
	obs := Observer{
		OnWrite:        func(int, int) { read(&b)() },
		OnFlush:        func(FlushInfo) { read(&b)() },
		OnDrop:         func(int, DropReason) { read(&b)() },
		OnDeadLetter:   func(int) { read(&b)() },
		OnBackpressure: func(State, Decision) { read(&b)() },
		OnCheckpoint:   func(uint64) { read(&b)() },
	}
	b = openBuf(t, t.TempDir(), sink, fast(WithObserver(obs))...)

	ctx := context.Background()
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("event-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b)

	if seen.Load() == 0 {
		t.Fatal("no callback managed to read Stats")
	}
}

// TestAsyncObserverSurvivesABlockingHook is the case a synchronous Observer
// cannot survive: a hook that blocks holds the delivery slot it was called from,
// and at the default MaxInFlight of 1 that stops the flusher outright.
func TestAsyncObserverSurvivesABlockingHook(t *testing.T) {
	sink := &recorder{}
	release := make(chan struct{})
	obs := Observer{OnFlush: func(FlushInfo) { <-release }}
	b := openBuf(t, t.TempDir(), sink, fast(WithAsyncObserver(obs, 64))...)

	ctx := context.Background()
	for i := range 50 {
		if err := b.Write(ctx, fmt.Sprintf("event-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush stalled behind a blocking hook: %v", err)
	}
	if got := b.Stats().PendingRecords; got != 0 {
		t.Fatalf("backlog is %d records; a blocking hook should not hold it up", got)
	}

	// Close drains the dispatcher under its own context and gives up when that
	// expires. Everything is durable by then, so a hook still wedged costs a
	// leaked goroutine and a missed metric, not a failed Close.
	cctx, ccancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer ccancel()
	start := time.Now()
	if err := b.Close(cctx); err != nil {
		t.Fatalf("Close failed on a hook that was still wedged: %v", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("Close waited %v on a wedged hook; the drain is meant to be bounded by ctx", waited)
	}
	close(release)
}

// TestAsyncObserverAllowsReentrantCalls covers the combination that deadlocks
// outright when the callbacks run inline: a hook calling Write against a full
// buffer under the default Block policy waits for capacity that only its own
// return can release.
func TestAsyncObserverAllowsReentrantCalls(t *testing.T) {
	sink := &recorder{}
	var b *Buffer[string]
	var writes, flushes atomic.Int64
	// Bounded: a hook that writes on every flush feeds itself another flush, and
	// left to spin it is a busy loop that perturbs the rest of the suite. Two
	// passes prove the point the test is making.
	var entered atomic.Int64
	obs := Observer{
		OnFlush: func(FlushInfo) {
			if entered.Add(1) > 2 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := b.Write(ctx, "from-hook"); err == nil || errors.Is(err, ErrClosed) {
				writes.Add(1)
			}
			if err := b.Flush(ctx); err == nil || errors.Is(err, ErrClosed) {
				flushes.Add(1)
			}
		},
	}
	b = openBuf(t, t.TempDir(), sink, fast(
		WithAsyncObserver(obs, 64),
		WithCapacity(3, 0),
		WithFlush(1, 0, 5*time.Millisecond),
	)...)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for i := range 3 {
		if err := b.Write(ctx, fmt.Sprintf("event-%d", i)); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for writes.Load() == 0 || flushes.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("re-entrant hooks never completed: Write=%d Flush=%d", writes.Load(), flushes.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	closeBuf(t, b)
}

// TestAsyncObserverDropsRatherThanBlocks: a hook that cannot keep up must cost
// metrics, not throughput or records.
func TestAsyncObserverDropsRatherThanBlocks(t *testing.T) {
	sink := &recorder{}
	obs := Observer{OnFlush: func(FlushInfo) { time.Sleep(20 * time.Millisecond) }}
	b := openBuf(t, t.TempDir(), sink, fast(
		WithAsyncObserver(obs, 1),
		WithFlush(1, 0, 2*time.Millisecond),
	)...)

	ctx := context.Background()
	for i := range 100 {
		if err := b.Write(ctx, fmt.Sprintf("event-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	st := b.Stats()
	if st.Flushed != 100 {
		t.Fatalf("delivered %d records, want 100: the queue must drop events, not records", st.Flushed)
	}
	if st.ObserverDropped == 0 {
		t.Fatal("a queue of 1 against a 20ms hook dropped nothing; ObserverDropped is not counting")
	}
	closeBuf(t, b)
}

// TestAsyncObserverRecoversAPanickingHook: one bad hook must not take every
// metric after it down with the dispatcher goroutine.
func TestAsyncObserverRecoversAPanickingHook(t *testing.T) {
	sink := &recorder{}
	var seen atomic.Int64
	obs := Observer{
		OnFlush: func(FlushInfo) {
			if seen.Add(1) == 1 {
				panic("hook is broken")
			}
		},
	}
	b := openBuf(t, t.TempDir(), sink, fast(
		WithAsyncObserver(obs, 64),
		WithFlush(1, 0, 2*time.Millisecond),
	)...)

	ctx := context.Background()
	for i := range 10 {
		if err := b.Write(ctx, fmt.Sprintf("event-%02d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for seen.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("the dispatcher stopped after the panic: only %d hooks ran", seen.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := b.Stats().ObserverDropped; got != 1 {
		t.Fatalf("ObserverDropped is %d, want 1: a panicking hook has to be visible", got)
	}
	closeBuf(t, b)
}

// TestObserverOptionsAreLastOneWins: WithObserver after WithAsyncObserver has to
// put the callbacks back on the caller's goroutine, or the option ordering lies.
func TestObserverOptionsAreLastOneWins(t *testing.T) {
	c := newCounters()
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(
		WithAsyncObserver(c.observer(), 64),
		WithObserver(c.observer()),
	)...)
	if b.obs != nil {
		t.Fatal("WithObserver did not turn the dispatcher back off")
	}
	closeBuf(t, b)
}
