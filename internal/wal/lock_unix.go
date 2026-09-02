//go:build unix

package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockDir takes an exclusive advisory lock on the log directory. The lock is
// held for as long as the returned file is open and is released by closing it,
// including when the process dies.
func lockDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: %s is already open by another process: %w", dir, err)
	}
	return f, nil
}
