//go:build !unix

package wal

import (
	"os"
	"path/filepath"
)

const lockEnforced = false

// lockDir is a no-op on platforms without flock. Those platforms are not
// supported: the single-writer requirement is then the caller's to enforce, and
// two writers on one directory interleave records.
func lockDir(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, "LOCK"), os.O_RDWR|os.O_CREATE, 0o644)
}
