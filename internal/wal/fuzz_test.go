package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// FuzzScanSegment feeds recovery's scanner arbitrary bytes as a segment file.
// Whatever it is given, it must not panic, must only ever report the damage
// errors, and must describe a prefix that scans clean on its own: that prefix
// is what recovery cuts the file back to.
func FuzzScanSegment(f *testing.F) {
	var valid []byte
	for i := range 5 {
		valid = appendRecord(valid, payload(i))
	}
	f.Add(valid)
	f.Add(valid[:len(valid)-3])
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0})
	f.Add(appendRecord(nil, make([]byte, 300)))

	const maxRecord = 256
	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), segmentName(1, 0))
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		count, good, _, err := scanSegment(path, maxRecord)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrRecordTooLarge) {
				t.Fatalf("scan returned %v, want only ErrCorrupt or ErrRecordTooLarge", err)
			}
			if errors.Is(err, ErrRecordTooLarge) {
				return
			}
		}
		if good < 0 || good > int64(len(data)) {
			t.Fatalf("good = %d, outside a file of %d bytes", good, len(data))
		}

		if err := os.Truncate(path, good); err != nil {
			t.Fatal(err)
		}
		count2, good2, torn2, err2 := scanSegment(path, maxRecord)
		if err2 != nil || torn2 || count2 != count || good2 != good {
			t.Fatalf("the prefix recovery would keep rescans as (%d, %d, torn=%v, %v), want (%d, %d, clean)",
				count2, good2, torn2, err2, count, good)
		}
	})
}

// FuzzRecover damages one byte of a real log and opens it. Recovery may refuse
// the log, or cut records away, but it must never hand back a record under a
// number that named a different one, and never reuse a number afterwards. With
// Salvage set it must always open.
func FuzzRecover(f *testing.F) {
	f.Add(uint8(30), uint32(0), byte(1), false)
	f.Add(uint8(30), uint32(100), byte(0x40), false)
	f.Add(uint8(30), uint32(100), byte(0x40), true)
	f.Add(uint8(30), uint32(250), byte(0xff), true)
	f.Add(uint8(3), uint32(70), byte(0x80), false)

	f.Fuzz(func(t *testing.T, n uint8, at uint32, xor byte, salvage bool) {
		records := 1 + int(n)%40
		if xor == 0 {
			xor = 1
		}
		dir := t.TempDir()
		opts := Options{SyncMode: SyncNever, MaxSegmentBytes: 128, CommitBytes: 1, MaxRecordBytes: 64}
		l, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := range records {
			if _, err := l.Append(context.Background(), payload(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}

		damage(t, dir, int64(at), xor)

		if salvage {
			opts.Salvage = func(Salvage) {}
		}
		l, err = Open(dir, opts)
		if err != nil {
			if salvage {
				t.Fatalf("Open with Salvage = %v, want it to open whatever the damage", err)
			}
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrRecordTooLarge) {
				t.Fatalf("Open = %v, want only ErrCorrupt or ErrRecordTooLarge", err)
			}
			return
		}
		defer l.Close()

		lsns, payloads := readAll(t, l, 0)
		checkRecords(t, lsns, payloads)
		for i := 1; i < len(lsns); i++ {
			if lsns[i] <= lsns[i-1] {
				t.Fatalf("LSNs out of order: %v", lsns)
			}
		}
		lsn, err := l.Append(context.Background(), payload(999))
		if err != nil {
			t.Fatal(err)
		}
		if lsn <= uint64(records) {
			t.Fatalf("Append after recovery got LSN %d, reusing a number up to %d", lsn, records)
		}
	})
}

// damage XORs one byte of the log's segments, taken together in order, at
// offset at modulo their total size.
func damage(t *testing.T, dir string, at int64, xor byte) {
	t.Helper()
	segs, _ := filepath.Glob(filepath.Join(dir, "*"+segmentSuffix))
	sort.Strings(segs)
	var total int64
	sizes := make([]int64, len(segs))
	for i, s := range segs {
		st, err := os.Stat(s)
		if err != nil {
			t.Fatal(err)
		}
		sizes[i] = st.Size()
		total += st.Size()
	}
	if total == 0 {
		return
	}
	at %= total
	for i, s := range segs {
		if at >= sizes[i] {
			at -= sizes[i]
			continue
		}
		f, err := os.OpenFile(s, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var b [1]byte
		if _, err := f.ReadAt(b[:], at); err != nil {
			t.Fatal(err)
		}
		b[0] ^= xor
		if _, err := f.WriteAt(b[:], at); err != nil {
			t.Fatal(err)
		}
		return
	}
}
