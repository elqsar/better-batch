package wal

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

// crashChildEnv makes the test binary re-exec as a writer that never stops, so
// the parent can kill it at an arbitrary point.
const crashChildEnv = "WAL_CRASH_CHILD_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(crashChildEnv); dir != "" {
		runCrashChild(dir)
		return
	}
	os.Exit(m.Run())
}

func runCrashChild(dir string) {
	l, err := Open(dir, Options{SyncMode: SyncAlways})
	if err != nil {
		os.Exit(2)
	}
	ctx := context.Background()
	for i := 0; ; i++ {
		if _, err := l.Append(ctx, payload(i)); err != nil {
			os.Exit(3)
		}
	}
}

// TestSurvivesSIGKILL is the test the whole design exists for: a real process is
// killed mid-write and the log must come back as a clean, gap-free prefix.
func TestSurvivesSIGKILL(t *testing.T) {
	dir := t.TempDir()

	cmd := exec.Command(os.Args[0], "-test.run=TestSurvivesSIGKILL")
	cmd.Env = append(os.Environ(), crashChildEnv+"="+dir)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start writer: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill writer: %v", err)
	}
	_ = cmd.Wait()

	// The lock must have been released by the dead process.
	l := open(t, dir, Options{})
	lsns, payloads := readAll(t, l, 0)
	if len(payloads) == 0 {
		t.Fatal("writer was killed before committing anything")
	}
	for i := range payloads {
		if lsns[i] != uint64(i+1) || !bytes.Equal(payloads[i], payload(i)) {
			t.Fatalf("record %d recovered as LSN %d %q, want LSN %d %q",
				i, lsns[i], payloads[i], i+1, payload(i))
		}
	}
	next, err := l.Append(context.Background(), payload(len(payloads)))
	if err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if want := uint64(len(payloads) + 1); next != want {
		t.Fatalf("LSN after recovery = %d, want %d", next, want)
	}
	t.Logf("recovered %d records written before SIGKILL", len(payloads))
}
