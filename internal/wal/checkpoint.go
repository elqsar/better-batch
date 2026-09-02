package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const checkpointName = "CHECKPOINT"

// WriteCheckpoint durably records the highest LSN that has been fully handled
// downstream. It is written to a temporary file and renamed, so a crash leaves
// either the old value or the new one, never a partial one.
//
// The value must be a low-water mark: with more than one batch in flight a
// later batch can finish first, and storing that would silently skip the
// earlier one after a crash.
func WriteCheckpoint(dir string, lsn uint64) error {
	tmp := filepath.Join(dir, checkpointName+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", lsn); err != nil {
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
	if err := os.Rename(tmp, filepath.Join(dir, checkpointName)); err != nil {
		return err
	}
	return syncDir(dir)
}

// ReadCheckpoint returns the stored checkpoint, or 0 if none has been written.
func ReadCheckpoint(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, checkpointName))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	lsn, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: malformed checkpoint: %w", err)
	}
	return lsn, nil
}
