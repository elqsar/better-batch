package batch

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/elqsar/better-batch/internal/wal"
)

const crashChildEnv = "BATCH_CRASH_CHILD_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(crashChildEnv); dir != "" {
		runCrashChild(dir)
		return
	}
	os.Exit(m.Run())
}

// fileSink records everything the sink accepted. A plain append is enough: the
// page cache survives SIGKILL, and this test is about process death, not power
// loss.
type fileSink struct {
	mu sync.Mutex
	f  *os.File
}

func (s *fileSink) Flush(_ context.Context, b Batch[string]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range b.Records {
		if _, err := s.f.WriteString(r + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func appendFile(t testing.TB, path string) *os.File {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			out = append(out, line)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// runCrashChild writes an unbounded stream of records, noting each one that
// Write accepted, until the parent kills it.
func runCrashChild(dir string) {
	sinkFile, err := os.OpenFile(filepath.Join(dir, "delivered"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		os.Exit(2)
	}
	accepted, err := os.OpenFile(filepath.Join(dir, "accepted"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		os.Exit(2)
	}
	b, err := Open[string](filepath.Join(dir, "buf"), &fileSink{f: sinkFile}, stringCodec{},
		WithSync(SyncAlways, 0),
		WithFlush(20, 0, 5*time.Millisecond),
		WithCheckpointInterval(10*time.Millisecond),
	)
	if err != nil {
		os.Exit(2)
	}
	ctx := context.Background()
	for i := 0; ; i++ {
		if err := b.Write(ctx, strconv.Itoa(i)); err != nil {
			os.Exit(3)
		}
		if _, err := accepted.WriteString(strconv.Itoa(i) + "\n"); err != nil {
			os.Exit(4)
		}
	}
}

// TestAtLeastOnceAcrossSIGKILL kills a real writer mid-stream and checks the
// contract the whole library exists to provide: every record Write accepted is
// eventually delivered, duplicates permitted.
func TestAtLeastOnceAcrossSIGKILL(t *testing.T) {
	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=TestAtLeastOnceAcrossSIGKILL")
	cmd.Env = append(os.Environ(), crashChildEnv+"="+dir)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start writer: %v", err)
	}
	time.Sleep(700 * time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill writer: %v", err)
	}
	_ = cmd.Wait()

	accepted := readLines(t, filepath.Join(dir, "accepted"))
	if len(accepted) < 10 {
		t.Fatalf("child only got %d writes in before it was killed; not a useful test", len(accepted))
	}

	// Reopen and let the replay finish.
	b, err := Open[string](filepath.Join(dir, "buf"), &fileSink{f: appendFile(t, filepath.Join(dir, "delivered"))},
		stringCodec{}, WithFlush(20, 0, 5*time.Millisecond), WithCheckpointInterval(10*time.Millisecond))
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.Flush(ctx); err != nil {
		t.Fatalf("Flush after crash: %v", err)
	}
	if err := b.Close(ctx); err != nil {
		t.Fatalf("Close after crash: %v", err)
	}

	delivered := readLines(t, filepath.Join(dir, "delivered"))
	seen := make(map[string]int, len(delivered))
	for _, d := range delivered {
		seen[d]++
	}

	var missing []string
	for _, a := range accepted {
		if seen[a] == 0 {
			missing = append(missing, a)
		}
	}
	if len(missing) > 0 {
		show := missing
		if len(show) > 10 {
			show = show[:10]
		}
		t.Fatalf("%d of %d accepted records were never delivered (first: %v); at-least-once was violated",
			len(missing), len(accepted), show)
	}

	dupes := 0
	for _, n := range seen {
		if n > 1 {
			dupes++
		}
	}
	t.Logf("accepted %d, delivered %d (%d duplicated by replay)", len(accepted), len(delivered), dupes)
}

// loseLogTail simulates a machine crash that took the page-cached tail of the
// log with it while the fsynced CHECKPOINT survived: every segment keeps only
// the given fraction of its bytes.
func loseLogTail(t *testing.T, dir string, keep float64) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".log" {
			continue
		}
		p := filepath.Join(dir, e.Name())
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(p, int64(float64(st.Size())*keep)); err != nil {
			t.Fatal(err)
		}
	}
}

// A checkpoint is always fsynced; under SyncNever the records it refers to are
// not. A machine crash can therefore leave the mark pointing past the end of the
// log. Left alone it starts the flusher above every LSN the log is about to hand
// out, so nothing written afterwards is ever read, let alone delivered.
func TestCheckpointAheadOfLogIsClamped(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	first := &recorder{}
	b := openBuf(t, dir, first, fast(WithSync(SyncAlways, 0))...)
	for i := range 100 {
		if err := b.Write(ctx, fmt.Sprintf("old%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	closeBuf(t, b)
	if got := len(first.seen()); got != 100 {
		t.Fatalf("delivered %d records before the crash, want 100", got)
	}

	loseLogTail(t, dir, 0.5)

	second := &recorder{}
	b2 := openBuf(t, dir, second, fast(WithSync(SyncAlways, 0))...)
	for i := range 10 {
		if err := b2.Write(ctx, fmt.Sprintf("new%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b2.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b2)

	// Every record at or below the stale mark was acked by the sink before the
	// crash, so clamping loses nothing and re-delivers nothing.
	got := second.seen()
	if len(got) != 10 {
		t.Fatalf("delivered %d records after reopen, want exactly the 10 new ones: %v", len(got), got)
	}
	for i, rec := range got {
		if want := fmt.Sprintf("new%d", i); rec != want {
			t.Fatalf("delivered[%d] = %q, want %q", i, rec, want)
		}
	}
	if s := b2.Stats(); s.PendingRecords != 0 {
		t.Fatalf("Stats.PendingRecords = %d after everything drained, want 0", s.PendingRecords)
	}
}

// The same recovery with nothing left of the log at all, which clamps to zero
// and takes the fresh-segment path through recovery.
func TestCheckpointWithoutLogSegmentsRecovers(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	b := openBuf(t, dir, &recorder{}, fast(WithSync(SyncAlways, 0))...)
	for i := range 20 {
		if err := b.Write(ctx, fmt.Sprintf("old%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	closeBuf(t, b)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	if cp, err := wal.ReadCheckpoint(dir); err != nil || cp == 0 {
		t.Fatalf("checkpoint = %d, %v; the test needs a surviving mark to clamp", cp, err)
	}

	sink := &recorder{}
	b2 := openBuf(t, dir, sink, fast()...)
	for i := range 5 {
		if err := b2.Write(ctx, fmt.Sprintf("new%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b2.Flush(fctx); err != nil {
		t.Fatal(err)
	}
	closeBuf(t, b2)

	if got := len(sink.seen()); got != 5 {
		t.Fatalf("delivered %d records, want 5; a mark left ahead of an empty log strands every write", got)
	}
}

type discard struct{}

func (discard) Flush(context.Context, Batch[string]) error { return nil }

func BenchmarkWrite(b *testing.B) {
	buf, err := Open[string](b.TempDir(), discard{}, stringCodec{},
		WithSync(SyncNever, 0), WithFlush(1000, 0, 10*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	defer buf.Close(context.Background())

	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		if err := buf.Write(ctx, strconv.Itoa(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/s")
}

func BenchmarkWriteBatch(b *testing.B) {
	for _, size := range []int{10, 100, 1000} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			buf, err := Open[string](b.TempDir(), discard{}, stringCodec{},
				WithSync(SyncAlways, 0), WithFlush(1000, 0, 10*time.Millisecond))
			if err != nil {
				b.Fatal(err)
			}
			defer buf.Close(context.Background())

			recs := make([]string, size)
			for i := range recs {
				recs[i] = strconv.Itoa(i)
			}
			ctx := context.Background()
			b.ResetTimer()
			for range b.N {
				if err := buf.WriteBatch(ctx, recs...); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size*b.N)/b.Elapsed().Seconds(), "records/s")
		})
	}
}
