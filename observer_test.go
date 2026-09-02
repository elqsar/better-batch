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
