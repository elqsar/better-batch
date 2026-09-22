package wal

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// writeSegments fills a log with n records across many small segments and
// returns the segment paths in order.
func writeSegments(t *testing.T, dir string, n int) []string {
	t.Helper()
	l, err := Open(dir, Options{SyncMode: SyncNever, MaxSegmentBytes: 256, CommitBytes: 1})
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
	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	sort.Strings(segs)
	if len(segs) < 3 {
		t.Fatalf("need at least 3 segments, got %d", len(segs))
	}
	return segs
}

// flipPayloadByte corrupts the payload of the record-th record in a segment,
// leaving intact records after it, which is interior corruption rather than a
// torn tail.
func flipPayloadByte(t *testing.T, path string, record int) int64 {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	stride := int64(headerSize + len(payload(0)))
	start := stride * int64(record)
	var b [1]byte
	if _, err := f.ReadAt(b[:], start+headerSize+2); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0x40
	if _, err := f.WriteAt(b[:], start+headerSize+2); err != nil {
		t.Fatal(err)
	}
	return start
}

// checkRecords asserts every record read back carries the payload its LSN was
// written with: salvage may lose records, never renumber them.
func checkRecords(t *testing.T, lsns []uint64, payloads [][]byte) {
	t.Helper()
	for i, lsn := range lsns {
		if want := payload(int(lsn - 1)); !bytes.Equal(payloads[i], want) {
			t.Fatalf("LSN %d holds %q, want %q", lsn, payloads[i], want)
		}
	}
}

func TestSalvageCutsInteriorCorruptionOutOfAMiddleSegment(t *testing.T) {
	dir := t.TempDir()
	const total = 100
	segs := writeSegments(t, dir, total)
	damaged := segs[1]
	before, err := os.ReadFile(damaged)
	if err != nil {
		t.Fatal(err)
	}
	offset := flipPayloadByte(t, damaged, 1)

	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open without Salvage = %v, want ErrCorrupt", err)
	}

	var reports []Salvage
	opts := Options{Salvage: func(s Salvage) { reports = append(reports, s) }}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open with Salvage: %v", err)
	}
	if len(reports) != 1 {
		t.Fatalf("Salvage called %d times, want once", len(reports))
	}
	r := reports[0]
	if r.Path != damaged || r.Offset != offset || r.DiscardedBytes != int64(len(before))-offset || !errors.Is(r.Cause, ErrCorrupt) {
		t.Fatalf("Salvage report = %+v, want %s cut at %d", r, damaged, offset)
	}
	kept, err := os.ReadFile(r.Kept)
	if err != nil {
		t.Fatal(err)
	}
	if want := before[offset:]; len(kept) != len(want) || !bytes.Equal(kept[headerSize+3:], want[headerSize+3:]) {
		t.Fatalf("kept file holds %d bytes, want the %d cut from the segment", len(kept), len(want))
	}

	lsns, payloads := readAll(t, l, 0)
	checkRecords(t, lsns, payloads)
	if lsns[len(lsns)-1] != total {
		t.Fatalf("reader ended at LSN %d, want it to step over the gap to %d", lsns[len(lsns)-1], total)
	}
	lost := total - len(lsns)
	if lost == 0 {
		t.Fatal("nothing was lost, so the corruption was not where the test put it")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// The damage is gone for good: a later Open, with or without the option,
	// sees the same log and salvages nothing.
	reports = nil
	for _, o := range []Options{opts, {}} {
		l2, err := Open(dir, o)
		if err != nil {
			t.Fatalf("reopen after salvage: %v", err)
		}
		again, _ := readAll(t, l2, 0)
		if len(again) != len(lsns) {
			t.Fatalf("reopen read %d records, want %d", len(again), len(lsns))
		}
		l2.Close()
	}
	if len(reports) != 0 {
		t.Fatalf("a salvaged log was salvaged again: %+v", reports)
	}
}

func TestSalvageOfTheLastSegmentNeverReusesLostLSNs(t *testing.T) {
	dir := t.TempDir()
	const total = 95 // leaves several records in the last segment
	segs := writeSegments(t, dir, total)
	// Corruption in the last segment with records after it is still interior
	// damage, not a torn tail.
	flipPayloadByte(t, segs[len(segs)-1], 0)

	// The reservation is what normally keeps lost numbers from coming back.
	// Take it away too: salvage has to bound them by itself.
	if err := os.Remove(filepath.Join(dir, reservedName)); err != nil {
		t.Fatal(err)
	}

	var called int
	l := open(t, dir, Options{Salvage: func(Salvage) { called++ }})
	if called != 1 {
		t.Fatalf("Salvage called %d times, want once", called)
	}
	lsns, payloads := readAll(t, l, 0)
	checkRecords(t, lsns, payloads)
	lsn, err := l.Append(context.Background(), payload(999))
	if err != nil {
		t.Fatal(err)
	}
	if lsn <= total {
		t.Fatalf("Append after salvage got LSN %d, reusing a number up to %d that named a lost record", lsn, total)
	}
	after, _ := readAll(t, l, 0)
	if after[len(after)-1] != lsn {
		t.Fatalf("reader ended at %d, want it to reach the new record at %d", after[len(after)-1], lsn)
	}
}

func TestSalvageOfAMiddleSegmentCutShort(t *testing.T) {
	dir := t.TempDir()
	const total = 100
	segs := writeSegments(t, dir, total)
	st, err := os.Stat(segs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(segs[1], st.Size()/2); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("Open without Salvage accepted a middle segment cut short")
	}
	var called int
	l := open(t, dir, Options{Salvage: func(Salvage) { called++ }})
	if called != 1 {
		t.Fatalf("Salvage called %d times, want once", called)
	}
	lsns, payloads := readAll(t, l, 0)
	checkRecords(t, lsns, payloads)
	if lsns[len(lsns)-1] != total {
		t.Fatalf("reader ended at LSN %d, want %d", lsns[len(lsns)-1], total)
	}
}

// A record that verifies but is above MaxRecordBytes was written under a larger
// limit. That is configuration, not damage, and salvage must not cut it away.
func TestSalvageLeavesOversizedRecordsAlone(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SyncMode: SyncAlways})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Append(context.Background(), make([]byte, 1024)); err != nil {
		t.Fatal(err)
	}
	l.Close()

	_, err = Open(dir, Options{MaxRecordBytes: 100, Salvage: func(Salvage) { t.Error("salvaged an intact record") }})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Open = %v, want ErrRecordTooLarge", err)
	}
}
