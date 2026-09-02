package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func open(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// readAll drains a log from fromLSN and returns the payloads it yielded.
func readAll(t *testing.T, l *Log, fromLSN uint64) (lsns []uint64, payloads [][]byte) {
	t.Helper()
	r, err := l.NewReader(fromLSN)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()
	for {
		lsn, p, err := r.Next()
		if errors.Is(err, ErrNoData) {
			return lsns, payloads
		}
		if err != nil {
			t.Fatalf("Next at LSN %d: %v", lsn, err)
		}
		lsns = append(lsns, lsn)
		payloads = append(payloads, bytes.Clone(p))
	}
}

func payload(i int) []byte { return []byte(fmt.Sprintf("record-%08d", i)) }

func TestAppendAndRead(t *testing.T) {
	l := open(t, t.TempDir(), Options{SyncMode: SyncAlways})
	ctx := context.Background()

	for i := range 100 {
		lsn, err := l.Append(ctx, payload(i))
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if want := uint64(i + 1); lsn != want {
			t.Fatalf("LSN = %d, want %d", lsn, want)
		}
	}

	lsns, payloads := readAll(t, l, 0)
	if len(payloads) != 100 {
		t.Fatalf("read %d records, want 100", len(payloads))
	}
	for i, p := range payloads {
		if !bytes.Equal(p, payload(i)) {
			t.Fatalf("record %d = %q, want %q", i, p, payload(i))
		}
		if lsns[i] != uint64(i+1) {
			t.Fatalf("record %d has LSN %d, want %d", i, lsns[i], i+1)
		}
	}
}

func TestAppendBatch(t *testing.T) {
	l := open(t, t.TempDir(), Options{SyncMode: SyncAlways})

	batch := [][]byte{payload(0), payload(1), payload(2)}
	first, err := l.AppendBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if first != 1 {
		t.Fatalf("first LSN = %d, want 1", first)
	}
	if got := l.DurableLSN(); got != 3 {
		t.Fatalf("DurableLSN = %d, want 3", got)
	}
	_, payloads := readAll(t, l, 0)
	if len(payloads) != 3 {
		t.Fatalf("read %d records, want 3", len(payloads))
	}
}

func TestReopenRecovers(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	l, err := Open(dir, Options{SyncMode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2 := open(t, dir, Options{SyncMode: SyncAlways})
	_, payloads := readAll(t, l2, 0)
	if len(payloads) != 50 {
		t.Fatalf("recovered %d records, want 50", len(payloads))
	}

	// Numbering must continue where it left off.
	lsn, err := l2.Append(ctx, payload(50))
	if err != nil {
		t.Fatal(err)
	}
	if lsn != 51 {
		t.Fatalf("LSN after reopen = %d, want 51", lsn)
	}
}

func TestSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{
		SyncMode:        SyncNever,
		MaxSegmentBytes: 512,
		CommitBytes:     1,
	})
	ctx := context.Background()

	const n = 200
	for i := range n {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			t.Fatal(err)
		}
	}

	segs, err := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) < 2 {
		t.Fatalf("got %d segments, want rotation to have produced several", len(segs))
	}

	// Reading must cross segment boundaries transparently.
	lsns, payloads := readAll(t, l, 0)
	if len(payloads) != n {
		t.Fatalf("read %d records across %d segments, want %d", len(payloads), len(segs), n)
	}
	for i := range payloads {
		if !bytes.Equal(payloads[i], payload(i)) || lsns[i] != uint64(i+1) {
			t.Fatalf("record %d mismatched after rotation", i)
		}
	}

	// And so must a reader that starts in the middle of a later segment.
	lsns, payloads = readAll(t, l, 150)
	if len(payloads) != n-149 {
		t.Fatalf("mid-log read returned %d records, want %d", len(payloads), n-149)
	}
	if lsns[0] != 150 || !bytes.Equal(payloads[0], payload(149)) {
		t.Fatalf("mid-log read started at LSN %d with %q", lsns[0], payloads[0])
	}
}

func TestTruncateDropsConsumedSegments(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{SyncMode: SyncNever, MaxSegmentBytes: 512, CommitBytes: 1})
	ctx := context.Background()

	for i := range 200 {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))

	if err := l.Truncate(150); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	if len(after) >= len(before) {
		t.Fatalf("Truncate removed no segments (%d -> %d)", len(before), len(after))
	}
	if got := l.FirstLSN(); got > 150 {
		t.Fatalf("FirstLSN = %d, truncation dropped records at or above the cutoff", got)
	}

	// Everything from the cutoff on must still be readable.
	lsns, _ := readAll(t, l, 150)
	if lsns[0] != 150 || lsns[len(lsns)-1] != 200 {
		t.Fatalf("after truncation the log spans %d..%d, want 150..200", lsns[0], lsns[len(lsns)-1])
	}

	// A reader asking for something already deleted must say so.
	if _, err := l.NewReader(1); !errors.Is(err, ErrTruncated) {
		t.Fatalf("NewReader(1) = %v, want ErrTruncated", err)
	}
}

// TestRecoverFromTornTail is the property that matters: whatever byte offset a
// crash lands on, reopening must succeed and return an uncorrupted prefix.
func TestRecoverFromTornTail(t *testing.T) {
	const n = 300
	rng := rand.New(rand.NewSource(1))

	for trial := range 40 {
		dir := t.TempDir()
		l, err := Open(dir, Options{SyncMode: SyncNever, CommitBytes: 1, MaxSegmentBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		for i := range n {
			if _, err := l.Append(context.Background(), payload(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}

		// Chop the last segment at an arbitrary point, as a crash mid-write would.
		segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
		sort.Strings(segs)
		last := segs[len(segs)-1]
		st, err := os.Stat(last)
		if err != nil {
			t.Fatal(err)
		}
		cut := rng.Int63n(st.Size() + 1)
		if err := os.Truncate(last, cut); err != nil {
			t.Fatal(err)
		}

		l2, err := Open(dir, Options{SyncMode: SyncNever, CommitBytes: 1})
		if err != nil {
			t.Fatalf("trial %d: reopen after truncation at %d: %v", trial, cut, err)
		}
		lsns, payloads := readAll(t, l2, 0)
		if len(payloads) == 0 {
			l2.Close()
			continue
		}
		for i := range payloads {
			if lsns[i] != uint64(i+1) || !bytes.Equal(payloads[i], payload(i)) {
				t.Fatalf("trial %d (cut %d): record %d is LSN %d %q, want LSN %d %q",
					trial, cut, i, lsns[i], payloads[i], i+1, payload(i))
			}
		}
		// Appends must resume immediately after the surviving prefix.
		next, err := l2.Append(context.Background(), payload(9999))
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(len(payloads) + 1); next != want {
			t.Fatalf("trial %d: LSN after recovery = %d, want %d", trial, next, want)
		}
		l2.Close()
	}
}

func TestBitFlipInsideTheLogIsRejected(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncMode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if _, err := l.Append(context.Background(), payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	f, err := os.OpenFile(segs[0], os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt a payload byte inside record 11 (records are headerSize+16 bytes).
	stride := int64(headerSize + len(payload(0)))
	var b [1]byte
	off := stride*10 + headerSize + 2
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x40
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Records 12-20 are intact and were acknowledged as durable, so this must
	// not be classified as a torn tail and silently truncated away.
	if _, err := Open(dir, Options{SyncMode: SyncAlways}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open over interior corruption = %v, want ErrCorrupt", err)
	}
}

func TestBrokenSegmentChainIsRejected(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncMode: SyncNever, MaxSegmentBytes: 512, CommitBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		if _, err := l.Append(context.Background(), payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	sort.Strings(segs)
	if len(segs) < 3 {
		t.Skipf("need at least 3 segments, got %d", len(segs))
	}
	if err := os.Remove(segs[1]); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("Open succeeded with a hole in the segment chain; LSNs would have silently shifted")
	}
}

func TestConcurrentAppendsAreUniqueAndComplete(t *testing.T) {
	l := open(t, t.TempDir(), Options{SyncInterval: time.Millisecond})

	const writers, each = 16, 200
	var wg sync.WaitGroup
	seen := make([][]uint64, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				lsn, err := l.Append(context.Background(), []byte(fmt.Sprintf("w%02d-%04d", w, i)))
				if err != nil {
					t.Errorf("writer %d: %v", w, err)
					return
				}
				seen[w] = append(seen[w], lsn)
			}
		}()
	}
	wg.Wait()

	all := map[uint64]bool{}
	for _, s := range seen {
		for _, lsn := range s {
			if all[lsn] {
				t.Fatalf("LSN %d handed out twice", lsn)
			}
			all[lsn] = true
		}
	}
	if len(all) != writers*each {
		t.Fatalf("got %d distinct LSNs, want %d", len(all), writers*each)
	}
	lsns, _ := readAll(t, l, 0)
	if len(lsns) != writers*each {
		t.Fatalf("log holds %d records, want %d", len(lsns), writers*each)
	}
}

func TestReaderFollowsWriter(t *testing.T) {
	l := open(t, t.TempDir(), Options{SyncMode: SyncAlways})
	ctx := context.Background()

	r, err := l.NewReader(0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	if _, _, err := r.Next(); !errors.Is(err, ErrNoData) {
		t.Fatalf("Next on empty log = %v, want ErrNoData", err)
	}
	for i := range 5 {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			t.Fatal(err)
		}
		lsn, p, err := r.Next()
		if err != nil {
			t.Fatalf("Next after append %d: %v", i, err)
		}
		if lsn != uint64(i+1) || !bytes.Equal(p, payload(i)) {
			t.Fatalf("tailing reader got LSN %d %q", lsn, p)
		}
		if _, _, err := r.Next(); !errors.Is(err, ErrNoData) {
			t.Fatalf("Next past the tail = %v, want ErrNoData", err)
		}
	}
}

func TestDirectoryLockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	l := open(t, dir, Options{})

	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("second Open succeeded; two writers would interleave records")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	l2.Close()
}

func TestRecordTooLarge(t *testing.T) {
	l := open(t, t.TempDir(), Options{MaxRecordBytes: 64})
	if _, err := l.Append(context.Background(), make([]byte, 65)); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Append oversized = %v, want ErrRecordTooLarge", err)
	}
}

func TestAppendRespectsContextCancellation(t *testing.T) {
	// A long sync interval means the append is still waiting when ctx expires.
	l := open(t, t.TempDir(), Options{SyncInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := l.Append(ctx, payload(0)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Append = %v, want DeadlineExceeded", err)
	}
	// The record was staged, so Close must still commit it.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2 := open(t, l.dir, Options{})
	if _, payloads := readAll(t, l2, 0); len(payloads) != 1 {
		t.Fatalf("recovered %d records; a cancelled append must still be durable once committed", len(payloads))
	}
}

// A caller that gives up waiting needs to tell "undecided" from "failed", so it
// knows whether the records may still land. Await carries both: ErrPending for
// the distinction and the context error for callers matching on cancellation.
func TestAwaitReportsPendingOnCancel(t *testing.T) {
	l := open(t, t.TempDir(), Options{SyncInterval: time.Hour})
	c, err := l.AppendBatchAsync([][]byte{payload(0), payload(1)})
	if err != nil {
		t.Fatalf("AppendBatchAsync: %v", err)
	}
	if c.First != 1 || c.Last != 2 {
		t.Fatalf("Commit = {%d, %d}, want {1, 2}", c.First, c.Last)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = l.Await(ctx, c)
	if !errors.Is(err, ErrPending) {
		t.Fatalf("Await on an unfinished round = %v, want ErrPending", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Await = %v, must still match the context error it was given", err)
	}

	// Abandoning the wait does not abandon the records: Close commits them.
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l2 := open(t, l.dir, Options{})
	if _, payloads := readAll(t, l2, 0); len(payloads) != 2 {
		t.Fatalf("recovered %d records, want 2; an abandoned wait must not lose staged records", len(payloads))
	}
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	got, err := ReadCheckpoint(dir)
	if err != nil || got != 0 {
		t.Fatalf("ReadCheckpoint on fresh dir = %d, %v; want 0, nil", got, err)
	}
	if err := WriteCheckpoint(dir, 4242); err != nil {
		t.Fatal(err)
	}
	if got, err = ReadCheckpoint(dir); err != nil || got != 4242 {
		t.Fatalf("ReadCheckpoint = %d, %v; want 4242, nil", got, err)
	}
}

func BenchmarkAppendSerial(b *testing.B) {
	for _, mode := range []struct {
		name string
		opts Options
	}{
		{"SyncAlways", Options{SyncMode: SyncAlways}},
		{"SyncPeriodic5ms", Options{SyncMode: SyncPeriodic, SyncInterval: 5 * time.Millisecond}},
		{"SyncNever", Options{SyncMode: SyncNever}},
	} {
		b.Run(mode.name, func(b *testing.B) {
			l, err := Open(b.TempDir(), mode.opts)
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			p := payload(0)
			ctx := context.Background()
			b.SetBytes(int64(len(p)))
			b.ResetTimer()
			for range b.N {
				if _, err := l.Append(ctx, p); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAppendParallel(b *testing.B) {
	l, err := Open(b.TempDir(), Options{SyncMode: SyncPeriodic, SyncInterval: 2 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	p := payload(0)
	b.SetBytes(int64(len(p)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := l.Append(ctx, p); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAppendBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("batch%d", size), func(b *testing.B) {
			l, err := Open(b.TempDir(), Options{SyncMode: SyncAlways})
			if err != nil {
				b.Fatal(err)
			}
			defer l.Close()
			batch := make([][]byte, size)
			for i := range batch {
				batch[i] = payload(i)
			}
			ctx := context.Background()
			b.SetBytes(int64(size * len(batch[0])))
			b.ResetTimer()
			for range b.N {
				if _, err := l.AppendBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}

// TestCommitFailureRollsBackAndRecovers covers a full disk: the write lands
// partially and then fails, and the log has to shed the fragment, hand the
// error to the appender, and keep working once space comes back.
func TestCommitFailureRollsBackAndRecovers(t *testing.T) {
	dir := t.TempDir()
	var failing atomic.Bool

	opts := Options{SyncMode: SyncAlways}
	opts.WriteFunc = func(f *os.File, b []byte) (int, error) {
		if !failing.Load() {
			return f.Write(b)
		}
		// A short write followed by ENOSPC is what a full disk actually does.
		n, _ := f.Write(b[:len(b)/2])
		return n, syscall.ENOSPC
	}

	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 10 {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			t.Fatalf("append %d before the disk filled: %v", i, err)
		}
	}

	failing.Store(true)
	if _, err := l.Append(ctx, payload(10)); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Append on a full disk = %v, want ENOSPC surfaced to the caller", err)
	}

	// Space comes back. The log must carry on from where it left off.
	failing.Store(false)
	lsn, err := l.Append(ctx, payload(10))
	if err != nil {
		t.Fatalf("Append after space was freed: %v", err)
	}
	if lsn != 11 {
		t.Fatalf("LSN after rollback = %d, want 11; a failed commit must give its LSNs back", lsn)
	}

	lsns, payloads := readAll(t, l, 0)
	if len(payloads) != 11 {
		t.Fatalf("log holds %d records, want 11", len(payloads))
	}
	for i := range payloads {
		if lsns[i] != uint64(i+1) || !bytes.Equal(payloads[i], payload(i)) {
			t.Fatalf("record %d is LSN %d %q, want LSN %d %q", i, lsns[i], payloads[i], i+1, payload(i))
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// And the fragment must not have survived on disk either.
	l2 := open(t, dir, Options{})
	lsns, payloads = readAll(t, l2, 0)
	if len(payloads) != 11 {
		t.Fatalf("after reopen the log holds %d records, want 11", len(payloads))
	}
	for i := range payloads {
		if lsns[i] != uint64(i+1) || !bytes.Equal(payloads[i], payload(i)) {
			t.Fatalf("after reopen record %d is LSN %d %q", i, lsns[i], payloads[i])
		}
	}
}

func TestRepeatedCommitFailuresStayConsistent(t *testing.T) {
	dir := t.TempDir()
	var failing atomic.Bool
	opts := Options{SyncMode: SyncAlways}
	opts.WriteFunc = func(f *os.File, b []byte) (int, error) {
		if failing.Load() {
			return 0, syscall.ENOSPC
		}
		return f.Write(b)
	}
	l := open(t, dir, opts)
	ctx := context.Background()

	want := 0
	for round := range 20 {
		failing.Store(round%2 == 1)
		_, err := l.Append(ctx, payload(want))
		if round%2 == 1 {
			if !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("round %d: got %v, want ENOSPC", round, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		want++
	}

	lsns, payloads := readAll(t, l, 0)
	if len(payloads) != want {
		t.Fatalf("log holds %d records, want %d", len(payloads), want)
	}
	for i := range payloads {
		if lsns[i] != uint64(i+1) || !bytes.Equal(payloads[i], payload(i)) {
			t.Fatalf("record %d is LSN %d %q, want LSN %d %q", i, lsns[i], payloads[i], i+1, payload(i))
		}
	}
}

func TestTruncateRetriesAfterRemoveFailure(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	l := open(t, dir, Options{MaxSegmentBytes: 64})
	for i := 0; i < 20; i++ {
		if _, err := l.Append(context.Background(), payload(i)); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	err := l.Truncate(15)
	if err == nil {
		os.Chmod(dir, 0o755)
		t.Fatal("Truncate succeeded despite an unwritable directory")
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	before, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if err := l.Truncate(15); err != nil {
		t.Fatalf("Truncate retry: %v", err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "*.log"))
	if len(after) >= len(before) {
		t.Fatalf("retry removed nothing: %d segments before, %d after", len(before), len(after))
	}

	// The surviving records must still read back.
	lsns, _ := readAll(t, l, 15)
	if len(lsns) == 0 || lsns[0] != 15 {
		t.Fatalf("read from 15 gave lsns %v", lsns)
	}
}
