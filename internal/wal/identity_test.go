package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// appendN writes n records and closes the log.
func appendN(t *testing.T, dir string, n int) (id string, last uint64) {
	t.Helper()
	l, err := Open(dir, Options{SyncMode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if last, err = l.Append(context.Background(), payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	id = l.ID()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return id, last
}

func TestIdentityIsStableAcrossReopens(t *testing.T) {
	dir := t.TempDir()
	first, _ := appendN(t, dir, 3)
	if first == "" {
		t.Fatal("new log has no identity")
	}
	again, last := appendN(t, dir, 1)
	if again != first {
		t.Fatalf("identity changed across a reopen: %q then %q", first, again)
	}
	if last != 4 {
		t.Fatalf("LSN after reopen = %d, want 4", last)
	}
	if other, _ := appendN(t, t.TempDir(), 1); other == first {
		t.Fatalf("two new logs share the identity %q", first)
	}
}

// The footgun the identity exists for: a directory emptied and reused numbers
// from 1 again, so its LSNs name different records than they used to. A LOGID
// file left behind must not carry over and vouch for them.
func TestIdentityChangesWhenNumberingStartsOver(t *testing.T) {
	dir := t.TempDir()
	old, _ := appendN(t, dir, 3)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != identityName {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}

	id, last := appendN(t, dir, 1)
	if last != 1 {
		t.Fatalf("emptied log numbered its first record %d, want 1", last)
	}
	if id == old {
		t.Fatalf("emptied log kept identity %q while its numbering restarted", id)
	}
}

// Losing the segments alone does not restart the numbering — the reservation
// says which LSNs are spent — so the identity stays with it.
func TestIdentitySurvivesLostSegments(t *testing.T) {
	dir := t.TempDir()
	old, before := appendN(t, dir, 3)
	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	for _, s := range segs {
		if err := os.Remove(s); err != nil {
			t.Fatal(err)
		}
	}
	id, last := appendN(t, dir, 1)
	if last <= before {
		t.Fatalf("LSN %d reused after the segments were lost (had reached %d)", last, before)
	}
	if id != old {
		t.Fatalf("identity changed from %q to %q though the numbering continued", old, id)
	}
}

// A log written before identities existed gets one, and keeps its numbering.
func TestIdentityIsAddedToAnExistingLog(t *testing.T) {
	dir := t.TempDir()
	appendN(t, dir, 3)
	if err := os.Remove(filepath.Join(dir, identityName)); err != nil {
		t.Fatal(err)
	}
	id, last := appendN(t, dir, 1)
	if id == "" || last != 4 {
		t.Fatalf("identity %q, LSN %d; want a new identity and LSN 4", id, last)
	}
	if again, _ := appendN(t, dir, 1); again != id {
		t.Fatalf("identity added on upgrade did not stick: %q then %q", id, again)
	}
}

func TestMalformedIdentityIsRejected(t *testing.T) {
	dir := t.TempDir()
	appendN(t, dir, 1)
	if err := os.WriteFile(filepath.Join(dir, identityName), []byte("\n"), fileMode); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, Options{})
	if err == nil {
		l.Close()
		t.Fatal("Open accepted an empty identity")
	}
	if !strings.Contains(err.Error(), "malformed log identity") || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want malformed log identity", err)
	}
}
