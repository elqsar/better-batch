package batch

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// A panic in code the buffer calls from its own goroutines has no frame of the
// caller's to be recovered in, so without the buffer's help it ends the
// process. What the buffer does instead has to keep every record: a panic is a
// bug, and a fixed build must still be able to deliver what the broken one
// could not.
func TestPanicInSinkOrHandlerFailsTheBufferAndKeepsRecords(t *testing.T) {
	panicking := SinkFunc[string](func(context.Context, Batch[string]) error { panic("sink bug") })
	rejecting := SinkFunc[string](func(context.Context, Batch[string]) error {
		return errors.Join(errSink, ErrPermanent)
	})
	failing := SinkFunc[string](func(context.Context, Batch[string]) error { return errSink })

	for _, tc := range []struct {
		name string
		sink Sink[string]
		opts []Option
	}{
		{name: "sink", sink: panicking},
		{name: "dead-letter sink", sink: rejecting, opts: []Option{WithDeadLetter[string](panicking)}},
		{name: "sink error handler", sink: failing, opts: []Option{
			WithOnSinkError(func(SinkFailure) RetryDecision { panic("handler bug") }),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			want := []string{"a", "b", "c", "d", "e"}
			b := openBuf(t, dir, tc.sink, fast(tc.opts...)...)
			if err := b.WriteBatch(context.Background(), want...); err != nil {
				t.Fatal(err)
			}

			waitFor(t, 5*time.Second, func() bool { return b.Stats().Err != nil })
			if err := b.Stats().Err; !errors.Is(err, ErrPanic) {
				t.Fatalf("Stats().Err = %v, want it to match ErrPanic", err)
			}
			if s := b.Stats(); s.Dropped != 0 || s.DeadLettered != 0 {
				t.Fatalf("Dropped = %d, DeadLettered = %d; a panic must dispose of nothing", s.Dropped, s.DeadLettered)
			}
			if err := b.Write(context.Background(), "late"); !errors.Is(err, ErrFailed) || !errors.Is(err, ErrPanic) {
				t.Fatalf("Write after the panic = %v, want ErrFailed wrapping ErrPanic", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := b.Close(ctx); !errors.Is(err, ErrPanic) {
				t.Fatalf("Close = %v, want the panic that failed the buffer", err)
			}

			healthy := &recorder{}
			b2 := openBuf(t, dir, healthy, fast()...)
			defer closeBuf(t, b2)
			waitFor(t, 5*time.Second, func() bool { return len(healthy.seen()) >= len(want) })
			if got := healthy.seen(); !slices.Equal(got, want) {
				t.Fatalf("after reopening the sink saw %q, want %q", got, want)
			}
		})
	}
}

// A decode handler that panics never said the record could go, so it is kept
// exactly as StopBuffer keeps it.
func TestPanicInDecodeHandlerKeepsTheRecord(t *testing.T) {
	dir := t.TempDir()
	want := []string{"a", "poison", "b"}

	down := &recorder{}
	down.failAll.Store(true)
	b := openBuf(t, dir, down, fast()...)
	if err := b.WriteBatch(context.Background(), want...); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = b.Close(ctx)

	second := &recorder{}
	b2, err := Open[string](dir, second, poisonCodec{}, fast(
		WithOnDecodeFailure(func(DecodeFailure) DecodeAction { panic("handler bug") }),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return b2.Stats().Err != nil })
	if err := b2.Stats().Err; !errors.Is(err, ErrPanic) {
		t.Fatalf("Stats().Err = %v, want it to match ErrPanic", err)
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ccancel()
	_ = b2.Close(cctx)

	third := &recorder{}
	b3 := openBuf(t, dir, third, fast()...)
	defer closeBuf(t, b3)
	rest := want[len(second.seen()):]
	waitFor(t, 5*time.Second, func() bool { return len(third.seen()) >= len(rest) })
	if got := slices.Concat(second.seen(), third.seen()); !slices.Equal(got, want) {
		t.Fatalf("across the opens the sinks saw %q, want %q", got, want)
	}
}

// A metrics hook is not worth the pipeline. Inline hooks run on the flusher and
// the deliveries too, so a panic there has to be recovered and counted, just as
// the asynchronous dispatcher does.
func TestPanicInInlineObserverIsCountedNotFatal(t *testing.T) {
	sink := &recorder{}
	b := openBuf(t, t.TempDir(), sink, fast(WithObserver(Observer{
		OnWrite: func(int, int) { panic("write hook bug") },
		OnFlush: func(FlushInfo) { panic("flush hook bug") },
	}))...)
	defer closeBuf(t, b)

	for _, v := range []string{"a", "b", "c"} {
		if err := b.Write(context.Background(), v); err != nil {
			t.Fatalf("Write = %v; a panicking hook must not fail the write", err)
		}
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sink.seen(); len(got) != 3 {
		t.Fatalf("sink saw %q, want all three records", got)
	}
	s := b.Stats()
	if s.Err != nil {
		t.Fatalf("Stats().Err = %v; a panicking hook must not fail the buffer", s.Err)
	}
	if s.ObserverDropped < 4 { // three writes plus at least one flush
		t.Fatalf("ObserverDropped = %d, want every panicking hook counted", s.ObserverDropped)
	}
}

// panicCodec is a codec with a bug: it panics on one payload instead of
// returning an error.
type panicCodec struct{}

func (panicCodec) Encode(dst []byte, v string) ([]byte, error) { return append(dst, v...), nil }
func (panicCodec) Decode(src []byte) (string, error) {
	if string(src) == "poison" {
		panic("codec bug")
	}
	return string(src), nil
}

// Decode runs on the flusher's goroutine, so a codec that panics would end the
// process. A panic on one payload is that payload failing to decode, and it has
// to take the same route: dropped by default, kept by StopBuffer.
func TestPanicInCodecIsADecodeFailure(t *testing.T) {
	t.Run("dropped by default", func(t *testing.T) {
		sink := &recorder{}
		b, err := Open[string](t.TempDir(), sink, panicCodec{}, fast()...)
		if err != nil {
			t.Fatal(err)
		}
		defer closeBuf(t, b)
		if err := b.WriteBatch(context.Background(), "a", "poison", "b"); err != nil {
			t.Fatal(err)
		}
		if err := b.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := sink.seen(); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("sink saw %q, want the records around the one the codec panicked on", got)
		}
		if s := b.Stats(); s.Dropped != 1 || s.Err != nil {
			t.Fatalf("Dropped = %d, Err = %v; want 1 and nil", s.Dropped, s.Err)
		}
	})

	t.Run("kept by StopBuffer", func(t *testing.T) {
		var cause error
		b, err := Open[string](t.TempDir(), &recorder{}, panicCodec{}, fast(
			WithOnDecodeFailure(func(f DecodeFailure) DecodeAction {
				cause = f.Err
				return StopBuffer
			}),
		)...)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Write(context.Background(), "poison"); err != nil {
			t.Fatal(err)
		}
		waitFor(t, 5*time.Second, func() bool { return b.Stats().Err != nil })
		if !errors.Is(cause, ErrPanic) || !errors.Is(b.Stats().Err, ErrPanic) {
			t.Fatalf("handler saw %v and Stats().Err is %v; want both to match ErrPanic", cause, b.Stats().Err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})
}
