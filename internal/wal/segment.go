package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const segmentSuffix = ".log"

// segment is one append-only file in the log. Segments are named by the LSN of
// their first record, so sorting file names sorts by LSN.
type segment struct {
	baseLSN uint64
	path    string

	// prevEnd is the last LSN of the segment that preceded this one when it was
	// created. Recovery checks it against the segment actually in front, which
	// is what tells a legitimate gap in the numbering — recovery resumes above
	// LSNs that may already have been handed out — from a segment that has gone
	// missing and would otherwise renumber everything after it.
	prevEnd uint64

	// f is the write handle. It is non-nil only for the active (last) segment;
	// readers open their own read-only handles.
	f *os.File

	// size is the number of durable bytes in the file: every byte below it is a
	// complete, checksum-verified record.
	size int64

	// count is the number of records in the segment.
	count uint64
}

// lastLSN returns the LSN of the final record, or baseLSN-1 when empty.
func (s *segment) lastLSN() uint64 { return s.baseLSN + s.count - 1 }

// segmentName is baseLSN and prevEnd, both zero-padded so that sorting names
// sorts by LSN.
func segmentName(baseLSN, prevEnd uint64) string {
	return fmt.Sprintf("%020d.%020d%s", baseLSN, prevEnd, segmentSuffix)
}

func parseSegmentName(name string) (baseLSN, prevEnd uint64, ok bool) {
	if !strings.HasSuffix(name, segmentSuffix) {
		return 0, 0, false
	}
	base, prev, found := strings.Cut(strings.TrimSuffix(name, segmentSuffix), ".")
	if !found {
		return 0, 0, false
	}
	b, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	p, err := strconv.ParseUint(prev, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return b, p, true
}

// createSegment creates and opens a new empty segment that follows prevEnd.
func createSegment(dir string, baseLSN, prevEnd uint64) (*segment, error) {
	path := filepath.Join(dir, segmentName(baseLSN, prevEnd))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &segment{baseLSN: baseLSN, prevEnd: prevEnd, path: path, f: f}, nil
}

// discard closes a segment and removes its file. It is for a segment that was
// created but never became part of the log, so both failures are moot: the
// caller is already returning the error that made the segment unwanted.
func (s *segment) discard() {
	if s.f != nil {
		s.f.Close()
		s.f = nil
	}
	os.Remove(s.path)
}

// scanSegment reads a segment from the start, verifying every record, and
// reports how many complete records it holds and where the last one ends.
//
// A bad record that reaches the end of the file is a torn tail — expected
// after a crash and reported via truncated rather than as an error. A bad
// record with intact data after it cannot be a tear: it is interior corruption,
// and silently truncating there would discard durable records the writer was
// told were safe, so it is reported as ErrCorrupt instead.
func scanSegment(path string, maxRecordBytes int) (count uint64, good int64, truncated bool, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, false, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return 0, 0, false, err
	}
	total := stat.Size()

	r := bufio.NewReaderSize(f, 1<<20)
	hdr := make([]byte, headerSize)
	var payload []byte

	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			// A clean end of file is not a truncation; anything shorter than a
			// full header is.
			if errors.Is(err, io.EOF) {
				return count, good, good != total, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return count, good, true, nil
			}
			return 0, 0, false, err
		}
		length, crc := decodeHeader(hdr)
		if length > uint32(maxRecordBytes) {
			// The length field itself is corrupt, so the next record boundary is
			// unrecoverable either way: whatever follows cannot be re-framed.
			// This is indistinguishable from a torn header, so it stays a
			// truncation rather than an error.
			return count, good, true, nil
		}
		if cap(payload) < int(length) {
			payload = make([]byte, length)
		}
		payload = payload[:length]
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return count, good, true, nil
			}
			return 0, 0, false, err
		}
		if checksum(length, payload) != crc {
			if end := good + recordSize(int(length)); end < total {
				return 0, 0, false, fmt.Errorf("%w: %s: record at offset %d fails its checksum with %d intact bytes after it",
					ErrCorrupt, path, good, total-end)
			}
			return count, good, true, nil
		}
		count++
		good += recordSize(int(length))
	}
}

// syncDir fsyncs a directory so that a file creation, rename or removal within
// it is itself durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
