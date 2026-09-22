package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Salvage describes damage that Options.Salvage cut out of the log.
type Salvage struct {
	// Path is the damaged segment. It now ends at Offset.
	Path string

	// Offset is where the first bad record started: every record before it
	// was intact and is kept.
	Offset int64

	// DiscardedBytes is how much was cut from Offset to the end of the segment.
	// The records in it cannot be counted, because the damage is what would
	// have said where each one ends.
	DiscardedBytes int64

	// Kept is the file the discarded bytes were copied to, for forensics or
	// for recovering records by hand. The log never reads or deletes it.
	Kept string

	// Cause is what recovery found.
	Cause error
}

// segName is a segment as its file name describes it.
type segName struct{ base, prevEnd uint64 }

// salvage cuts a damaged segment back to its last good record and turns the
// records lost with the damage into a gap.
//
// The order is what makes a crash part-way through safe to repeat. The damaged
// bytes are copied aside first, so nothing is destroyed before it is kept. The
// following segment is relinked next, while the damaged one still fails its
// scan: a crash after the relink only means the next Open salvages again and
// finds the link already made. The cut comes last.
//
// A damaged last segment has no successor to bound the lost numbers with, so
// the reservation is raised past as many records as the discarded bytes could
// possibly have held. The log resumes above it and no lost number is handed out
// twice, even if the reservation file itself did not survive.
func (l *Log) salvage(path string, base, count uint64, good int64, next *segName, cause error) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	discarded := st.Size() - good

	kept := fmt.Sprintf("%s.corrupt-%d-%d", path, good, time.Now().UnixNano())
	if err := keepBytes(f, good, kept); err != nil {
		return fmt.Errorf("wal: salvage %s: keep damaged bytes: %w", path, err)
	}

	lastGood := base + count - 1 // base-1 when nothing before the damage survived
	if next != nil && next.prevEnd != lastGood {
		from := filepath.Join(l.dir, segmentName(next.base, next.prevEnd))
		to := filepath.Join(l.dir, segmentName(next.base, lastGood))
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("wal: salvage %s: relink %s: %w", path, from, err)
		}
		next.prevEnd = lastGood
	}
	if next == nil {
		if bound := lastGood + uint64(discarded/headerSize) + 1; bound > l.reserved {
			if err := l.writeReserved(bound); err != nil {
				return fmt.Errorf("wal: salvage %s: reserve lost LSNs: %w", path, err)
			}
			l.reserved = bound
		}
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}

	if err := f.Truncate(good); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	l.opts.Salvage(Salvage{Path: path, Offset: good, DiscardedBytes: discarded, Kept: kept, Cause: cause})
	return nil
}

// keepBytes copies src from off to its end into a new file at path, durably.
func keepBytes(src *os.File, off int64, path string) error {
	dst, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, fileMode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, io.NewSectionReader(src, off, 1<<62)); err != nil {
		dst.Close()
		return err
	}
	if err := dst.Sync(); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}
