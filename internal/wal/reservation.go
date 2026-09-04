package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const reservedName = "RESERVED"

// reserveBlock is how many LSNs are claimed per durable reservation. The fsync
// it costs is amortised across the whole block, and the only price of claiming
// too many is a gap in the numbering after a crash.
const reserveBlock = 1 << 20

// writeReserved durably records the highest LSN that may have been handed out.
//
// An LSN must never name two different records: a sink that has seen one can be
// asked to deduplicate against it, and the checkpoint that says it was
// delivered is fsynced even when the records themselves are not. After a crash
// the log therefore resumes above this mark rather than renumbering from the
// surviving tail. Written to a temporary file and renamed, so a crash leaves
// either the old value or the new one.
func (l *Log) writeReserved(lsn uint64) error {
	dir := l.dir
	tmp := filepath.Join(dir, reservedName+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	write := f.Write
	if l.opts.WriteFunc != nil {
		write = func(b []byte) (int, error) { return l.opts.WriteFunc(f, b) }
	}
	if _, err := write(fmt.Appendf(nil, "%d\n", lsn)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, reservedName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// readReserved returns the stored reservation, or 0 if none has been written.
func readReserved(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, reservedName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	lsn, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: malformed reservation: %w", err)
	}
	return lsn, nil
}
