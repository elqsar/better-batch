package batch_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	batch "github.com/elqsar/better-batch"
)

type Event struct {
	Name string
	N    int
}

func Example() {
	dir, err := os.MkdirTemp("", "batch-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// A sink receives batches. Returning nil means the records are safely
	// downstream and the buffer may forget them.
	//
	// Whatever a sink does, it must not block on something only the caller can
	// release: the buffer waits for the sink, so a full channel nobody is
	// draining yet would stop Close from ever returning.
	var delivered atomic.Int64
	sink := batch.SinkFunc[Event](func(ctx context.Context, b batch.Batch[Event]) error {
		delivered.Add(int64(b.Len()))
		return nil
	})

	buf, err := batch.Open[Event](dir, sink, batch.JSONCodec[Event]{},
		// Batch out at 100 records, 1 MiB, or every 50ms, whichever comes first.
		batch.WithFlush(100, 1<<20, 50*time.Millisecond),
		// Cap the unflushed backlog.
		batch.WithCapacity(10_000, 64<<20),
		// Apply backpressure for five seconds, then start shedding the backlog
		// rather than taking the application down with the sink.
		batch.WithOnFull(batch.BlockThenDropOldest(5*time.Second)),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	for i := range 250 {
		if err := buf.Write(ctx, Event{Name: "click", N: i}); err != nil {
			panic(err)
		}
	}

	// Close drains whatever is still buffered before returning.
	if err := buf.Close(ctx); err != nil {
		panic(err)
	}

	fmt.Println("delivered", delivered.Load())
	// Output: delivered 250
}

// ExampleWithObserver shows the shape of a metrics integration. Real wiring for
// Prometheus and OpenTelemetry is in the README; the mechanism is the same,
// only the recording calls differ.
func ExampleWithObserver() {
	dir, err := os.MkdirTemp("", "batch-observer")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	var records, batches, dropped atomic.Int64
	var slowestFlush atomic.Int64

	observer := batch.Observer{
		OnWrite: func(n, bytes int) {
			records.Add(int64(n))
		},
		OnFlush: func(f batch.FlushInfo) {
			if f.Err != nil {
				return // count errors separately in real code
			}
			batches.Add(1)
			for {
				prev := slowestFlush.Load()
				if int64(f.Duration) <= prev || slowestFlush.CompareAndSwap(prev, int64(f.Duration)) {
					break
				}
			}
		},
		OnDrop: func(n int, reason batch.DropReason) {
			dropped.Add(int64(n))
		},
	}

	buf, err := batch.Open[string](dir,
		batch.SinkFunc[string](func(context.Context, batch.Batch[string]) error { return nil }),
		batch.StringCodec{},
		batch.WithFlush(100, 0, time.Hour), // size-triggered only, for a stable example
		batch.WithObserver(observer),
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	for i := range 300 {
		if err := buf.Write(ctx, fmt.Sprintf("event-%d", i)); err != nil {
			panic(err)
		}
	}
	if err := buf.Close(ctx); err != nil {
		panic(err)
	}

	// Gauges come from Stats, which is meant to be polled; the Observer covers
	// things that happen rather than things that are.
	fmt.Printf("records=%d batches=%d dropped=%d pending=%d\n",
		records.Load(), batches.Load(), dropped.Load(), buf.Stats().PendingRecords)
	// Output: records=300 batches=3 dropped=0 pending=0
}
