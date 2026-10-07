package wal

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const identityName = "LOGID"

// An LSN names one record only within one numbering, and a numbering starts
// over whenever a log is created from nothing — a new directory, or an old one
// emptied and reused. A sink that deduplicates on LSNs alone across such a
// restart would take a new record for one it has already stored and silently
// discard it. The identity names the numbering, so the pair (identity, LSN) is
// what names one record anywhere.

// loadIdentity returns the log's identity, creating it when there is none or
// when fresh says the numbering is about to start from LSN 1. A stale identity
// left behind by a directory that was emptied by hand must not survive into
// the new numbering, or it would vouch for LSNs that now name other records.
func loadIdentity(dir string, fresh bool) (string, error) {
	if !fresh {
		b, err := os.ReadFile(filepath.Join(dir, identityName))
		switch {
		case err == nil:
			id := strings.TrimSpace(string(b))
			if id == "" || strings.ContainsFunc(id, func(r rune) bool { return r <= ' ' || r > '~' }) {
				return "", fmt.Errorf("wal: malformed log identity %q", id)
			}
			return id, nil
		case !os.IsNotExist(err):
			return "", err
		}
		// A log written before identities existed: it gets one now. Batches
		// retried across this upgrade carry a different identity than their
		// first attempt did, which costs at most one duplicate — what
		// at-least-once already allows.
	}
	id := rand.Text()
	if err := writeIdentity(dir, id); err != nil {
		return "", err
	}
	return id, nil
}

// writeIdentity durably stores id. Written to a temporary file and renamed, so
// a crash leaves either no identity — and the next Open makes one before any
// record can be numbered under it — or a whole one.
func writeIdentity(dir, id string) error {
	tmp := filepath.Join(dir, identityName+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%s\n", id); err != nil {
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
	if err := os.Rename(tmp, filepath.Join(dir, identityName)); err != nil {
		return err
	}
	return syncDir(dir)
}
