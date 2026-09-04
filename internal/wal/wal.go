// Package wal implements the segmented append-only log that backs better-batch's
// durable buffer.
//
// The log is a sequence of numbered segment files. Records are appended with a
// length and a CRC, and every record gets a monotonically increasing log
// sequence number (LSN) starting at 1. LSNs are implicit: a record's LSN is its
// segment's base LSN plus its index within that segment.
//
// Appends from many goroutines are batched into a single write and a single
// fsync (group commit), so the cost of durability is amortised across concurrent
// writers rather than paid per record.
//
// A log directory has a single writer, enforced with a lock file.
package wal

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

// Errors returned by the log and its readers.
var (
	ErrClosed         = errors.New("wal: log is closed")
	ErrNoData         = errors.New("wal: no more records")
	ErrRecordTooLarge = errors.New("wal: record exceeds MaxRecordBytes")
	ErrTruncated      = errors.New("wal: records have been truncated away")
	ErrCorrupt        = errors.New("wal: corrupt record")

	// ErrPending means the wait was abandoned before the records' commit round
	// finished. They are still staged and may yet become durable, so the caller
	// must not treat them as lost.
	ErrPending = errors.New("wal: commit round still in flight")
)

// SyncMode controls when committed records are forced to stable storage.
type SyncMode int

const (
	// SyncPeriodic fsyncs on a timer or once enough bytes have accumulated.
	// A crash loses at most the records written since the last commit round.
	SyncPeriodic SyncMode = iota

	// SyncAlways fsyncs every commit round. Concurrent appends still share a
	// single fsync, but an append never returns before its own fsync completes.
	SyncAlways

	// SyncNever hands records to the operating system but never fsyncs. A
	// process crash loses nothing; a machine crash may lose recent records.
	SyncNever
)

// Options configure a log. The zero value is valid and uses the defaults
// documented on each field.
type Options struct {
	// SyncMode selects the durability policy. Default SyncPeriodic.
	SyncMode SyncMode

	// SyncInterval bounds how long a record can sit uncommitted in
	// SyncPeriodic and SyncNever modes. Default 5ms.
	SyncInterval time.Duration

	// MaxSegmentBytes is a soft cap on segment size. Rotation happens between
	// commit rounds, never mid-record, so a segment may overshoot by up to one
	// commit batch. Default 64 MiB.
	MaxSegmentBytes int64

	// MaxRecordBytes is the largest payload the log accepts. It also bounds how
	// much a corrupt length field can make recovery allocate. Default 4 MiB.
	MaxRecordBytes int

	// CommitBytes forces an early commit round once this many bytes are
	// pending, bounding both commit latency and segment overshoot. Default 1 MiB.
	CommitBytes int

	// WriteFunc replaces the writes the log makes to disk: segment appends and
	// the LSN reservation. It exists so tests can inject partial writes and I/O
	// errors; production code leaves it nil. The package is internal, so this
	// never reaches the library's public API.
	WriteFunc func(*os.File, []byte) (int, error)

	// SyncFunc replaces the fsync of a segment, for the same reason: a failure
	// there has to be reachable from a test, because it is the point where a
	// half-finished rotation would otherwise be left behind. Nil in production.
	SyncFunc func(*os.File) error
}

func (o Options) withDefaults() Options {
	if o.SyncInterval <= 0 {
		o.SyncInterval = 5 * time.Millisecond
	}
	if o.MaxSegmentBytes <= 0 {
		o.MaxSegmentBytes = 64 << 20
	}
	if o.MaxRecordBytes <= 0 {
		o.MaxRecordBytes = 4 << 20
	}
	if o.CommitBytes <= 0 {
		o.CommitBytes = 1 << 20
	}
	return o
}

// Log is a durable, segmented, append-only log. It is safe for concurrent use.
type Log struct {
	dir  string
	opts Options
	lock *os.File

	mu     sync.Mutex
	segs   []*segment
	active *segment

	nextLSN  uint64 // LSN the next appended record will get
	firstLSN uint64 // lowest LSN still on disk
	reserved uint64 // highest LSN durably claimed, never handed out twice

	pending    []byte // records staged for the next commit round
	spare      []byte // buffer handed back by the committer, reused for staging
	pendingMin uint64
	pendingMax uint64

	durable    uint64        // highest LSN that has completed a commit round
	commitWait chan struct{} // closed at the end of every commit round
	closed     bool

	// epoch increments whenever a failed commit is rolled back. A waiter whose
	// epoch no longer matches had its record discarded and must be told so.
	epoch   uint64
	failErr error
	// broken is set only when a rollback itself failed, so the segment may hold
	// garbage that later appends would bury. That case really is terminal.
	broken   bool
	closeErr error

	signal chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
}

// Open opens or creates a log in dir, recovering any existing segments.
//
// Recovery scans and checksum-verifies every segment. A partial or corrupt
// record at the tail of the final segment is expected after a crash and is
// truncated away; damage anywhere earlier is reported as an error, because the
// implicit LSN numbering cannot be reconstructed across a hole.
func Open(dir string, opts Options) (*Log, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{
		dir:        dir,
		opts:       opts,
		lock:       lock,
		commitWait: make(chan struct{}),
		signal:     make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	if err := l.recover(); err != nil {
		lock.Close()
		return nil, err
	}

	l.wg.Add(1)
	go l.committer()
	return l, nil
}

func (l *Log) recover() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	type found struct{ base, prevEnd uint64 }
	var names []found
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if base, prevEnd, ok := parseSegmentName(e.Name()); ok {
			names = append(names, found{base, prevEnd})
		}
	}
	slices.SortFunc(names, func(a, b found) int { return cmp.Compare(a.base, b.base) })

	reserved, err := readReserved(l.dir)
	if err != nil {
		return err
	}
	l.reserved = reserved

	for i, n := range names {
		path := filepath.Join(l.dir, segmentName(n.base, n.prevEnd))
		count, good, torn, err := scanSegment(path, l.opts.MaxRecordBytes)
		if err != nil {
			return err
		}
		last := i == len(names)-1
		if torn && !last {
			return fmt.Errorf("wal: segment %s is truncated but is not the last segment", path)
		}
		if torn {
			if err := os.Truncate(path, good); err != nil {
				return err
			}
		}
		s := &segment{baseLSN: n.base, prevEnd: n.prevEnd, path: path, size: good, count: count}
		if i > 0 {
			prev := l.segs[i-1]
			if prev.lastLSN() != s.prevEnd {
				return fmt.Errorf("wal: segment chain broken: %s ends at LSN %d but %s expects to follow %d",
					prev.path, prev.lastLSN(), path, s.prevEnd)
			}
		}
		l.segs = append(l.segs, s)
	}

	switch {
	case len(l.segs) == 0:
		// Every segment file is gone, but a surviving reservation still says
		// which numbers are spent, so the log resumes above them. prevEnd stays
		// 0: nothing precedes this segment on disk, and declaring the whole span
		// below it a gap is what lets a reader that starts down there — at a
		// checkpoint written before the files vanished — step up to it instead
		// of reporting records truncated away. Nothing is durable, so the log is
		// empty rather than durable up to the reservation.
		s, err := createSegment(l.dir, l.reserved+1, 0)
		if err != nil {
			return err
		}
		l.segs = append(l.segs, s)

	default:
		tail := l.segs[len(l.segs)-1].lastLSN()
		if l.reserved > tail {
			// LSNs above the surviving tail were handed out before the crash and
			// may already be downstream, named in a batch the sink deduplicates
			// against. Reusing them would make one LSN mean two different
			// records, so the log resumes above the reservation and leaves a gap.
			s, err := createSegment(l.dir, l.reserved+1, tail)
			if err != nil {
				return err
			}
			l.segs = append(l.segs, s)
		} else {
			last := l.segs[len(l.segs)-1]
			f, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			last.f = f
		}
	}

	l.active = l.segs[len(l.segs)-1]
	l.firstLSN = l.segs[0].baseLSN
	l.nextLSN = l.active.baseLSN + l.active.count
	// Durability is about records, not about how far the numbering has moved:
	// the segment recovery opens above the reservation is empty, and on a later
	// reopen its position would otherwise be mistaken for a durable record.
	l.durable = lastRecordLSN(l.segs)
	return nil
}

// Append stages a record and returns once it is durable, along with its LSN.
//
// If ctx is cancelled while waiting for the commit round, Append returns
// ErrPending wrapping the context error, but the record has still been staged
// and will be committed: the returned LSN is valid and the caller must treat the
// write as possibly durable.
func (l *Log) Append(ctx context.Context, payload []byte) (uint64, error) {
	if len(payload) > l.opts.MaxRecordBytes {
		return 0, ErrRecordTooLarge
	}
	lsn, epoch, force, err := l.stage(func(dst []byte) []byte { return appendRecord(dst, payload) }, 1)
	if err != nil {
		return 0, err
	}
	if force {
		l.signalCommit()
	}
	return lsn, l.waitDurable(ctx, lsn, epoch)
}

// Commit identifies a group of staged records so that waiting for their fate is
// separable from staging them. A caller that gives up waiting can hand the
// handle to something else, which is the only way to learn whether records it
// stopped waiting for were committed or rolled back.
type Commit struct {
	First, Last uint64

	// epoch pins the commit generation the records were staged in. An LSN alone
	// is not enough: a rollback hands the LSNs back, so a later record can take
	// the same number and look durable to a stale waiter.
	epoch uint64
}

// AppendBatch stages several records under a single lock acquisition and a
// single commit round, and returns the LSN of the first. The records are
// numbered consecutively from there.
func (l *Log) AppendBatch(ctx context.Context, payloads [][]byte) (uint64, error) {
	if len(payloads) == 0 {
		return 0, nil
	}
	c, err := l.AppendBatchAsync(payloads)
	if err != nil {
		return 0, err
	}
	return c.First, l.Await(ctx, c)
}

// AppendBatchAsync stages several records under a single lock acquisition and a
// single commit round, and returns without waiting for them to become durable.
// The caller must Await the returned Commit to learn their fate.
func (l *Log) AppendBatchAsync(payloads [][]byte) (Commit, error) {
	if len(payloads) == 0 {
		return Commit{}, nil
	}
	for _, p := range payloads {
		if len(p) > l.opts.MaxRecordBytes {
			return Commit{}, ErrRecordTooLarge
		}
	}
	first, epoch, force, err := l.stage(func(dst []byte) []byte {
		for _, p := range payloads {
			dst = appendRecord(dst, p)
		}
		return dst
	}, uint64(len(payloads)))
	if err != nil {
		return Commit{}, err
	}
	if force {
		l.signalCommit()
	}
	return Commit{First: first, Last: first + uint64(len(payloads)) - 1, epoch: epoch}, nil
}

// Await returns nil once the records in c are durable, the commit error if they
// were rolled back, or ErrPending wrapped around ctx.Err() if ctx expired while
// their fate was still undecided. In that last case the records remain staged
// and may yet be committed, so the caller must not treat them as lost.
func (l *Log) Await(ctx context.Context, c Commit) error {
	return l.waitDurable(ctx, c.Last, c.epoch)
}

// stage encodes records into the pending buffer and assigns their LSNs.
func (l *Log) stage(encode func([]byte) []byte, n uint64) (first, epoch uint64, force bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, 0, false, ErrClosed
	}
	if l.broken {
		return 0, 0, false, l.failErr
	}
	// LSNs are claimed durably before they are handed out, in blocks so the
	// fsync costs one round per reserveBlock records. Without this a crash that
	// loses the log tail would renumber new records over LSNs the sink has
	// already seen.
	if last := l.nextLSN + n - 1; last > l.reserved {
		want := max(last, l.nextLSN+reserveBlock-1)
		if err := l.writeReserved(want); err != nil {
			return 0, 0, false, fmt.Errorf("wal: reserve LSNs: %w", err)
		}
		l.reserved = want
	}
	epoch = l.epoch
	first = l.nextLSN
	if len(l.pending) == 0 {
		l.pendingMin = first
	}
	l.pending = encode(l.pending)
	l.nextLSN += n
	l.pendingMax = first + n - 1
	// Only SyncPeriodic waits for a tick: it is the mode that trades latency for
	// fewer fsyncs. SyncAlways and SyncNever have nothing to amortise, so they
	// commit as soon as there is something to commit.
	force = l.opts.SyncMode != SyncPeriodic || len(l.pending) >= l.opts.CommitBytes
	return first, epoch, force, nil
}

func (l *Log) waitDurable(ctx context.Context, lsn, epoch uint64) error {
	for {
		l.mu.Lock()
		// Durability is checked first: a record that made it to disk stays
		// successful even if a later commit round failed and rolled back.
		if l.durable >= lsn {
			l.mu.Unlock()
			return nil
		}
		if l.epoch != epoch {
			err := l.failErr
			l.mu.Unlock()
			return err
		}
		ch := l.commitWait
		l.mu.Unlock()

		select {
		case <-ch:
		case <-ctx.Done():
			// The records stay staged, so this is "undecided", not "failed".
			// ErrPending says so; the ctx error is kept for callers matching on
			// cancellation.
			return fmt.Errorf("%w: %w", ErrPending, ctx.Err())
		}
	}
}

func (l *Log) hasPending() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending) > 0
}

func (l *Log) signalCommit() {
	select {
	case l.signal <- struct{}{}:
	default:
	}
}

// Sync forces a commit round and waits for everything staged so far to become
// durable.
func (l *Log) Sync(ctx context.Context) error {
	l.mu.Lock()
	target, epoch := l.pendingMax, l.epoch
	l.mu.Unlock()
	l.signalCommit()
	return l.waitDurable(ctx, target, epoch)
}

func (l *Log) committer() {
	defer l.wg.Done()

	var tick <-chan time.Time
	if l.opts.SyncMode != SyncAlways {
		t := time.NewTicker(l.opts.SyncInterval)
		defer t.Stop()
		tick = t.C
	}
	for {
		// SyncAlways and SyncNever commit as fast as the device allows rather
		// than on a schedule. Looping straight into the next round while data is
		// waiting is what makes group commit work: everything that arrives
		// during one write or fsync rides along in the next one.
		if l.opts.SyncMode != SyncPeriodic && l.hasPending() {
			l.commit()
			continue
		}
		select {
		case <-l.done:
			// Final round, so Close drains what is staged. Its error is the
			// only one Close reports: earlier failures were already handed to
			// the appenders they belonged to.
			if err := l.commit(); err != nil {
				l.mu.Lock()
				l.closeErr = err
				l.mu.Unlock()
			}
			return
		case <-l.signal:
			l.commit()
		case <-tick:
			l.commit()
		}
	}
}

// commit writes and syncs everything staged since the previous round. It runs
// only on the committer goroutine, so segment mutation needs no extra lock
// beyond publishing the result under l.mu.
//
// A failed round is rolled back rather than fatal: the segment is truncated
// back to its last durable byte and the LSNs of the discarded records are
// handed out again. That is what makes a full disk something the caller can
// react to instead of the end of the log's life.
func (l *Log) commit() error {
	l.mu.Lock()
	if len(l.pending) == 0 || l.broken {
		l.mu.Unlock()
		return nil
	}
	buf := l.pending
	first, last := l.pendingMin, l.pendingMax
	l.pending, l.spare = l.spare[:0], nil
	l.pendingMin, l.pendingMax = 0, 0
	l.mu.Unlock()

	err := l.writeBatch(buf, first, last-first+1)

	var rollbackErr error
	if err != nil {
		// The write may have landed partially. Cutting the segment back to its
		// last durable byte removes the fragment, so the next round appends
		// exactly where the last good record ended.
		rollbackErr = l.rollback()
	}

	l.mu.Lock()
	if err != nil {
		l.epoch++
		l.failErr = err
		// Anything staged while this round was in flight was numbered above the
		// records that just vanished, so it has to go too; its appenders learn
		// about it through the epoch change.
		l.pending = l.pending[:0]
		l.pendingMin, l.pendingMax = 0, 0
		l.nextLSN = first
		if rollbackErr != nil {
			// The segment may still hold a fragment that later appends would
			// bury beyond recovery's reach. This one really is terminal.
			l.broken = true
			l.failErr = errors.Join(err, rollbackErr)
		}
	} else {
		l.durable = last
	}
	l.spare = buf[:0]
	close(l.commitWait)
	l.commitWait = make(chan struct{})
	out := l.failErr
	if err == nil {
		out = nil
	}
	l.mu.Unlock()
	return out
}

// rollback cuts the active segment back to the last byte that was durable.
func (l *Log) rollback() error {
	l.mu.Lock()
	active, size := l.active, l.active.size
	l.mu.Unlock()
	if active.f == nil {
		return nil
	}
	if err := active.f.Truncate(size); err != nil {
		return err
	}
	// The file is opened O_APPEND, so the next write lands at the new end.
	return active.f.Sync()
}

// sync fsyncs a segment file, through the test hook when one is installed.
func (l *Log) sync(f *os.File) error {
	if l.opts.SyncFunc != nil {
		return l.opts.SyncFunc(f)
	}
	return f.Sync()
}

func (l *Log) writeBatch(buf []byte, first, n uint64) error {
	l.mu.Lock()
	active := l.active
	rotate := active.size > 0 && active.size+int64(len(buf)) > l.opts.MaxSegmentBytes
	l.mu.Unlock()

	if rotate {
		next, err := createSegment(l.dir, first, first-1)
		if err != nil {
			return err
		}
		// The new segment exists before the old one is closed, so any failure
		// in between has to take it away again: the retry after the rollback
		// asks for the same name, and O_EXCL would turn one bad fsync into a
		// log that can never rotate.
		if err := l.sync(active.f); err != nil {
			next.discard()
			return err
		}
		if err := active.f.Close(); err != nil {
			next.discard()
			return err
		}
		l.mu.Lock()
		active.f = nil
		l.segs = append(l.segs, next)
		l.active = next
		l.mu.Unlock()
		active = next
	}

	write := active.f.Write
	if l.opts.WriteFunc != nil {
		write = func(b []byte) (int, error) { return l.opts.WriteFunc(active.f, b) }
	}
	if _, err := write(buf); err != nil {
		return err
	}
	if l.opts.SyncMode != SyncNever {
		if err := l.sync(active.f); err != nil {
			return err
		}
	}

	// Publishing size and count last is what keeps readers from ever seeing a
	// byte that is not yet durable.
	l.mu.Lock()
	active.size += int64(len(buf))
	active.count += n
	l.mu.Unlock()
	return nil
}

// Truncate deletes every segment whose records are all below upto. The active
// segment is never deleted, so the log always keeps at least one segment.
func (l *Log) Truncate(upto uint64) error {
	l.mu.Lock()
	drop := 0
	for i := 0; i < len(l.segs)-1; i++ {
		if l.segs[i].lastLSN() >= upto {
			break
		}
		drop++
	}
	doomed := l.segs[:drop]
	l.mu.Unlock()

	if len(doomed) == 0 {
		return nil
	}
	// Files go first: a segment leaves the in-memory list only once its file is
	// gone, so a failed removal is retried by the next Truncate instead of
	// leaving an orphan on disk until the next Open.
	removed := 0
	var rerr error
	for _, s := range doomed {
		if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
			rerr = err
			break
		}
		removed++
	}

	l.mu.Lock()
	l.segs = l.segs[removed:]
	if len(l.segs) > 0 {
		l.firstLSN = l.segs[0].baseLSN
	}
	l.mu.Unlock()

	if rerr != nil {
		return rerr
	}
	return syncDir(l.dir)
}

// DurableLSN returns the highest LSN that has completed a commit round.
func (l *Log) DurableLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}

// FirstLSN returns the lowest LSN still present on disk.
func (l *Log) FirstLSN() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.firstLSN
}

// Close drains staged records, syncs, and releases the directory lock.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	l.mu.Unlock()

	close(l.done)
	l.wg.Wait()

	l.mu.Lock()
	err := l.closeErr
	active := l.active
	allocated, reserved := l.nextLSN-1, l.reserved
	l.mu.Unlock()

	// A clean shutdown knows exactly which LSNs were used, so it gives the
	// unused remainder of the block back. Without this every restart would skip
	// a block, and the numbering of a log that was closed properly should just
	// continue.
	//
	// What it gives back is bounded by what was handed out, not by what was
	// written: an empty segment opened above the reservation has already claimed
	// its base, so lowering the mark past it would let a later run believe those
	// numbers are still free.
	if reserved > allocated {
		if rerr := l.writeReserved(allocated); err == nil {
			err = rerr
		}
	}

	if active != nil && active.f != nil {
		if serr := active.f.Sync(); err == nil {
			err = serr
		}
		if cerr := active.f.Close(); err == nil {
			err = cerr
		}
		active.f = nil
	}
	if cerr := l.lock.Close(); err == nil { // releases the flock
		err = cerr
	}
	return err
}

// segView is an immutable snapshot of a segment, so readers never touch the
// writer's mutable state.
type segView struct {
	path    string
	baseLSN uint64
	prevEnd uint64
	count   uint64
	size    int64
}

// gapBefore reports whether lsn falls in the unused stretch in front of this
// segment: a number recovery skipped rather than one whose record was deleted.
func (v segView) gapBefore(lsn uint64) bool { return lsn > v.prevEnd && lsn < v.baseLSN }

func (l *Log) snapshot() ([]segView, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	views := make([]segView, len(l.segs))
	for i, s := range l.segs {
		views[i] = segView{path: s.path, baseLSN: s.baseLSN, prevEnd: s.prevEnd, count: s.count, size: s.size}
	}
	return views, l.durable
}
