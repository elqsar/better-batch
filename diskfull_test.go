package batch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// withWriteFault swaps the log's segment write so a test can simulate a full
// disk. It lives in a test file so it stays out of the library.
func withWriteFault(fn func(*os.File, []byte) (int, error)) Option {
	return func(c *config) { c.wal.WriteFunc = fn }
}

// enospcAfter returns a write function that starts failing once the flag is set.
func enospcAfter(failing *atomic.Bool) func(*os.File, []byte) (int, error) {
	return func(f *os.File, b []byte) (int, error) {
		if failing.Load() {
			return 0, syscall.ENOSPC
		}
		return f.Write(b)
	}
}

func TestDiskFullRejectSurfacesErrDiskFull(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)

	b := openBuf(t, t.TempDir(), &recorder{}, fast(
		WithOnFull(Reject()),
		withWriteFault(enospcAfter(&failing)),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	err := b.Write(context.Background(), "no room")
	if !errors.Is(err, ErrDiskFull) {
		t.Fatalf("Write on a full disk = %v, want ErrDiskFull", err)
	}
	if s := b.Stats(); s.DiskFullEvents == 0 {
		t.Fatal("Stats.DiskFullEvents is 0; a full disk must be visible in metrics")
	}
}

func TestDiskFullDropNewestKeepsWritesSucceeding(t *testing.T) {
	var failing atomic.Bool
	failing.Store(true)

	b := openBuf(t, t.TempDir(), &recorder{}, fast(
		WithOnFull(DropNewest()),
		withWriteFault(enospcAfter(&failing)),
	)...)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	}()

	ctx := context.Background()
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("e%d", i)); err != nil {
			t.Fatalf("DropNewest must never fail a write, got %v on record %d", err, i)
		}
	}
	if s := b.Stats(); s.Dropped != 20 {
		t.Fatalf("Stats.Dropped = %d, want 20", s.Dropped)
	}
}

// TestDiskFullBlocksThenRecovers is the case the buffer previously could not
// survive at all: a failed commit used to poison the log for good.
func TestDiskFullBlocksThenRecovers(t *testing.T) {
	var failing atomic.Bool
	sink := &recorder{}

	b := openBuf(t, t.TempDir(), sink, fast(
		WithOnFull(Block()),
		withWriteFault(enospcAfter(&failing)),
	)...)
	ctx := context.Background()

	for i := range 10 {
		if err := b.Write(ctx, fmt.Sprintf("before-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	failing.Store(true)
	// Free the "disk" shortly after the writer has parked on it.
	go func() {
		time.Sleep(150 * time.Millisecond)
		failing.Store(false)
	}()

	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := b.Write(wctx, "after-recovery"); err != nil {
		t.Fatalf("Write did not recover once space came back: %v", err)
	}

	fctx, fcancel := context.WithTimeout(ctx, 10*time.Second)
	defer fcancel()
	if err := b.Flush(fctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	closeBuf(t, b)

	seen := sink.seen()
	if len(seen) != 11 {
		t.Fatalf("sink got %d records, want 11 (10 before the disk filled plus the one after)", len(seen))
	}
	if seen[len(seen)-1] != "after-recovery" {
		t.Fatalf("last record delivered was %q, want after-recovery", seen[len(seen)-1])
	}
}

// TestDiskFullDoesNotCorruptTheLog checks that records the log rejected leave no
// trace: the surviving records must stay contiguous and replay cleanly.
func TestDiskFullDoesNotCorruptTheLog(t *testing.T) {
	dir := t.TempDir()
	var failing atomic.Bool
	sink := &recorder{}
	sink.failAll.Store(true) // nothing gets acknowledged, so everything replays

	b := openBuf(t, dir, sink, fast(
		WithSync(SyncAlways, 0),
		WithOnFull(Reject()),
		WithRetry(Backoff{Initial: time.Second, Jitter: 0}, 0),
		withWriteFault(enospcAfter(&failing)),
	)...)

	ctx := context.Background()
	var accepted []string
	for i := range 40 {
		failing.Store(i%3 == 1) // the disk comes and goes
		rec := fmt.Sprintf("e%02d", i)
		err := b.Write(ctx, rec)
		switch {
		case err == nil:
			accepted = append(accepted, rec)
		case errors.Is(err, ErrDiskFull):
		default:
			t.Fatalf("record %d: %v", i, err)
		}
	}
	failing.Store(false)
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_ = b.Close(cctx)

	if len(accepted) == 0 {
		t.Fatal("no write was accepted; the fault injector never let anything through")
	}

	// Reopen with a working sink: every accepted record must still be there.
	up := &recorder{}
	b2 := openBuf(t, dir, up, fast()...)
	fctx, fcancel := context.WithTimeout(ctx, 10*time.Second)
	defer fcancel()
	if err := b2.Flush(fctx); err != nil {
		t.Fatalf("Flush after reopen: %v", err)
	}
	closeBuf(t, b2)

	got := up.seen()
	if len(got) != len(accepted) {
		t.Fatalf("replayed %d records, want the %d that Write accepted", len(got), len(accepted))
	}
	for i := range got {
		if got[i] != accepted[i] {
			t.Fatalf("record %d replayed as %q, want %q; the log lost or reordered records around the failures",
				i, got[i], accepted[i])
		}
	}
}
