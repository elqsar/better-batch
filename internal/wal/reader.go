package wal

import (
	"os"
)

// readWindow is how much of a segment a Reader pulls in per syscall. Records
// are small and read strictly in order, so one large read amortises the syscall
// across hundreds of them.
const readWindow = 256 << 10

// Reader is a sequential cursor over the log. It reads only durable records, so
// it never observes a write that a crash could take back.
//
// A Reader is not safe for concurrent use, but any number of independent
// Readers may run alongside the writer.
type Reader struct {
	l *Log

	views   []segView
	durable uint64

	nextLSN uint64 // LSN the next call to Next will return
	idx     int    // index into views of the currently open segment
	f       *os.File
	path    string // path of the open file, to detect a segment change
	off     int64  // byte offset of the record at posLSN
	posLSN  uint64

	// win buffers a run of bytes from the current segment. Bytes below a
	// segment's durable size never change, so a cached window is always valid.
	win    []byte
	winOff int64
}

// NewReader returns a cursor positioned at fromLSN. Passing 0 starts at the
// oldest record still on disk.
func (l *Log) NewReader(fromLSN uint64) (*Reader, error) {
	r := &Reader{l: l, winOff: -1}
	r.refresh()
	if fromLSN == 0 {
		if len(r.views) > 0 {
			fromLSN = r.views[0].baseLSN
		} else {
			fromLSN = 1
		}
	}
	if len(r.views) > 0 && fromLSN < r.views[0].baseLSN {
		return nil, ErrTruncated
	}
	r.nextLSN = fromLSN
	return r, nil
}

func (r *Reader) refresh() {
	r.views, r.durable = r.l.snapshot()
}

// Next returns the next record and its LSN, or ErrNoData once the reader has
// caught up with the durable tail. Callers may retry after new records land.
//
// The returned slice aliases the reader's buffer and is only valid until the
// next call to Next.
func (r *Reader) Next() (uint64, []byte, error) {
	if r.nextLSN > r.durable {
		r.refresh()
		if r.nextLSN > r.durable {
			return 0, nil, ErrNoData
		}
	}
	if err := r.position(); err != nil {
		return 0, nil, err
	}
	v := r.views[r.idx]

	if r.off+headerSize > v.size {
		// Either this segment is spent, or the active segment grew since the
		// snapshot. Both are resolved by re-reading the segment list.
		r.refresh()
		if err := r.position(); err != nil {
			return 0, nil, err
		}
		v = r.views[r.idx]
		if r.off+headerSize > v.size {
			return 0, nil, ErrNoData
		}
	}

	hdr, err := r.at(r.off, headerSize, v.size)
	if err != nil {
		return 0, nil, err
	}
	length, crc := decodeHeader(hdr)
	end := r.off + recordSize(int(length))
	if length > uint32(r.l.opts.MaxRecordBytes) || end > v.size {
		return 0, nil, ErrCorrupt
	}

	// Fetch header and payload together so a record straddling the window edge
	// costs one refill rather than two.
	rec, err := r.at(r.off, headerSize+int(length), v.size)
	if err != nil {
		return 0, nil, err
	}
	payload := rec[headerSize:]
	if checksum(length, payload) != crc {
		return 0, nil, ErrCorrupt
	}

	lsn := r.nextLSN
	r.off = end
	r.nextLSN++
	r.posLSN = r.nextLSN
	return lsn, payload, nil
}

// at returns n bytes of the current segment starting at off, reading from the
// file only when the window does not already cover them.
func (r *Reader) at(off int64, n int, limit int64) ([]byte, error) {
	if off < r.winOff || off+int64(n) > r.winOff+int64(len(r.win)) {
		if err := r.fill(off, n, limit); err != nil {
			return nil, err
		}
	}
	start := int(off - r.winOff)
	return r.win[start : start+n], nil
}

func (r *Reader) fill(off int64, need int, limit int64) error {
	size := max(readWindow, need)
	if off+int64(size) > limit {
		size = int(limit - off)
	}
	if size < need {
		return ErrNoData // not that much durable data exists yet
	}
	if cap(r.win) < size {
		r.win = make([]byte, size)
	}
	r.win = r.win[:size]
	if _, err := r.f.ReadAt(r.win, off); err != nil {
		r.dropWindow()
		return err
	}
	r.winOff = off
	return nil
}

func (r *Reader) dropWindow() {
	r.win, r.winOff = r.win[:0], -1
}

// NextLSN reports where the cursor currently sits.
func (r *Reader) NextLSN() uint64 { return r.nextLSN }

// Close releases the reader's file handle.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// position makes sure the right segment is open and r.off points at r.nextLSN.
func (r *Reader) position() error {
	if i, ok := r.find(r.nextLSN); ok {
		return r.openAt(i)
	}
	r.refresh()
	if i, ok := r.find(r.nextLSN); ok {
		return r.openAt(i)
	}
	if len(r.views) > 0 && r.nextLSN < r.views[0].baseLSN {
		return ErrTruncated
	}
	return ErrNoData
}

func (r *Reader) find(lsn uint64) (int, bool) {
	for i, v := range r.views {
		if lsn >= v.baseLSN && lsn < v.baseLSN+v.count {
			return i, true
		}
	}
	return 0, false
}

func (r *Reader) openAt(i int) error {
	v := r.views[i]
	if r.f != nil && r.path == v.path && r.posLSN == r.nextLSN {
		r.idx = i
		return nil
	}
	if r.f != nil {
		r.f.Close()
		r.f = nil
	}
	f, err := os.Open(v.path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrTruncated
		}
		return err
	}
	r.f, r.path, r.idx = f, v.path, i
	r.dropWindow()

	off, err := r.skipTo(v, r.nextLSN)
	if err != nil {
		f.Close()
		r.f = nil
		return err
	}
	r.off, r.posLSN = off, r.nextLSN
	return nil
}

// skipTo walks record headers from the start of a segment to find the byte
// offset of target. Records were checksum-verified when they were written or
// recovered, so the headers can be trusted to chain correctly.
func (r *Reader) skipTo(v segView, target uint64) (int64, error) {
	var off int64
	for lsn := v.baseLSN; lsn < target; lsn++ {
		hdr, err := r.at(off, headerSize, v.size)
		if err != nil {
			return 0, err
		}
		length, _ := decodeHeader(hdr)
		off += recordSize(int(length))
	}
	return off, nil
}
