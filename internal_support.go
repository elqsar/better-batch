package batch

import (
	"math/rand"
	"sync"
	"time"
)

// gate is a broadcast that late waiters cannot miss: take the channel first,
// then check the condition, then wait. A signal between the check and the wait
// still wakes the waiter, because the channel it holds is the one that closes.
type gate struct {
	mu sync.Mutex
	ch chan struct{}
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) wait() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ch
}

func (g *gate) signal() {
	g.mu.Lock()
	close(g.ch)
	g.ch = make(chan struct{})
	g.mu.Unlock()
}

// ackTracker turns out-of-order batch completions into a low-water mark: the
// highest LSN below which every record has been handled.
//
// Storing the last completed LSN instead would silently skip an earlier batch
// that is still in flight when a crash happens, which is the classic way to
// lose data in a pipeline that looks like it works.
type ackTracker struct {
	mu   sync.Mutex
	low  uint64
	done map[uint64]uint64 // firstLSN -> lastLSN, completed ahead of the mark
}

func newAckTracker(low uint64) *ackTracker {
	return &ackTracker{low: low, done: make(map[uint64]uint64)}
}

// ack records that every LSN in [first, last] is handled and returns the
// resulting low-water mark.
func (a *ackTracker) ack(first, last uint64) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if first != a.low+1 {
		a.done[first] = last
		return a.low
	}
	a.low = last
	for {
		next, ok := a.done[a.low+1]
		if !ok {
			return a.low
		}
		delete(a.done, a.low+1)
		a.low = next
	}
}

func (a *ackTracker) mark() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.low
}

// ageTracker approximates how long the oldest unflushed record has been
// waiting. It keeps an ordered queue of (lsn, time) marks, each meaning "every
// record up to lsn was accepted no later than time". Marks are coalesced so the
// queue stays small; the coarsening only ever overstates an age, and only for
// records behind the head — the head, which is what OldestAge reports, keeps
// the resolution it was recorded with.
//
// A single transition-based timestamp is not enough here: it measures time
// since the backlog was last empty, so under sustained load it grows without
// bound and BlockThenDropOldest would shed fresh records immediately.
type ageTracker struct {
	mu    sync.Mutex
	marks []ageMark
}

type ageMark struct {
	lsn uint64 // highest LSN this mark covers
	at  int64  // unix nanos; no covered record was accepted before this
}

const (
	ageGranularity = int64(100 * time.Millisecond)
	ageMaxMarks    = 512
)

// note records that every LSN up to lsn was accepted by now.
func (a *ageTracker) note(lsn uint64, now int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := len(a.marks); n > 0 {
		if lsn <= a.marks[n-1].lsn {
			return
		}
		if now-a.marks[n-1].at < ageGranularity {
			// Extend the newest mark. Its older timestamp can only overstate
			// the age of the absorbed records, and by less than the granularity.
			a.marks[n-1].lsn = lsn
			return
		}
	}
	if len(a.marks) >= ageMaxMarks {
		// Halve the resolution by merging adjacent pairs, keeping each pair's
		// earlier timestamp so no age is ever understated.
		k := 0
		for i := 0; i < len(a.marks); i += 2 {
			m := a.marks[i]
			if i+1 < len(a.marks) {
				m.lsn = a.marks[i+1].lsn
			}
			a.marks[k] = m
			k++
		}
		a.marks = a.marks[:k]
	}
	a.marks = append(a.marks, ageMark{lsn: lsn, at: now})
}

// trim drops marks wholly covered by the low-water mark.
func (a *ageTracker) trim(mark uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := 0
	for i < len(a.marks) && a.marks[i].lsn <= mark {
		i++
	}
	if i > 0 {
		a.marks = append(a.marks[:0], a.marks[i:]...)
	}
}

// oldest returns the age of the oldest record not yet acknowledged, zero when
// nothing is pending.
func (a *ageTracker) oldest(now int64) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.marks) == 0 {
		return 0
	}
	return time.Duration(now - a.marks[0].at)
}

// Backoff describes the delay between flush attempts.
type Backoff struct {
	Initial time.Duration // delay before the second attempt; default 100ms
	Max     time.Duration // ceiling; default 30s
	Factor  float64       // multiplier per attempt; default 2
	Jitter  float64       // fraction of the delay to randomise; default 0.2
}

func (b Backoff) withDefaults() Backoff {
	if b.Initial <= 0 {
		b.Initial = 100 * time.Millisecond
	}
	if b.Max <= 0 {
		b.Max = 30 * time.Second
	}
	if b.Factor <= 1 {
		b.Factor = 2
	}
	if b.Jitter < 0 || b.Jitter > 1 {
		b.Jitter = 0.2
	}
	return b
}

// delay returns how long to wait before the given attempt (1 = the first retry).
func (b Backoff) delay(attempt int) time.Duration {
	d := float64(b.Initial)
	for range attempt - 1 {
		d *= b.Factor
		if d >= float64(b.Max) {
			d = float64(b.Max)
			break
		}
	}
	if b.Jitter > 0 {
		// Spread retries so a fleet recovering from one outage does not
		// stampede the sink in lockstep.
		d *= 1 - b.Jitter + 2*b.Jitter*rand.Float64()
	}
	return time.Duration(d)
}
