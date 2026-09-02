//go:build !unix

package wal

import (
	"os"
	"path/filepath"
)

// lockDir is a no-op on platforms without flock. The single-writer requirement
// is then the caller's to enforce.
func lockDir(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, "LOCK"), os.O_RDWR|os.O_CREATE, 0o644)
}
