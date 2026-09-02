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

func segmentName(baseLSN uint64) string {
	return fmt.Sprintf("%020d%s", baseLSN, segmentSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(name, segmentSuffix), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// createSegment creates and opens a new empty segment.
func createSegment(dir string, baseLSN uint64) (*segment, error) {
	path := filepath.Join(dir, segmentName(baseLSN))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return &segment{baseLSN: baseLSN, path: path, f: f}, nil
}

// scanSegment reads a segment from the start, verifying every record, and
// reports how many complete records it holds and where the last one ends.
//
// A partial or corrupt record at the tail is expected after a crash and is
// reported via truncated rather than as an error; only I/O failures are errors.
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
			// Only a corrupt header can produce this, so treat the rest of the
			// file as lost.
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
