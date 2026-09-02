package batch

import (
	"context"
	"time"
)

// Decision is what a Policy tells the buffer to do about a write that does not
// fit within the configured capacity.
type Decision int

const (
	// DecisionBlock waits for the sink to free space, then reconsiders. The
	// write still fails if its context is cancelled.
	DecisionBlock Decision = iota

	// DecisionReject fails the write with ErrFull and leaves the caller to
	// decide what to do.
	DecisionReject

	// DecisionDropNewest discards the incoming write and reports success. The
	// only decision that loses data silently, so it is never the default.
	DecisionDropNewest

	// DecisionDropOldest discards the oldest unflushed records to make room,
	// which is what a telemetry pipeline usually wants: fresh data matters
	// more than the backlog. Space is reclaimed by the flusher, so the writer
	// still waits briefly.
	DecisionDropOldest
)

// String makes a Decision usable directly as a metric label.
func (d Decision) String() string {
	switch d {
	case DecisionBlock:
		return "block"
	case DecisionReject:
		return "reject"
	case DecisionDropNewest:
		return "drop_newest"
	case DecisionDropOldest:
		return "drop_oldest"
	default:
		return "unknown"
	}
}

// State describes the buffer at the moment a write did not fit. A custom policy
// can use it to shed by age, by pressure, or by how badly the sink is doing.
type State struct {
	// Records and Bytes are the unflushed backlog.
	Records int64
	Bytes   int64

	// MaxRecords and MaxBytes are the configured limits. Zero means unlimited.
	MaxRecords int64
	MaxBytes   int64

	// OldestAge is how long the oldest unflushed record has been waiting.
	OldestAge time.Duration

	// SinkFailures counts consecutive failed flush attempts. Nonzero means the
	// destination is unhealthy, not merely slow.
	SinkFailures int64

	// DiskFull is true when the log itself could not grow, rather than the
	// configured capacity being reached. Records and Bytes may be well under
	// their limits: the filesystem is the constraint. Only the flusher draining
	// and truncating segments can clear it, so Reject and DropNewest are the
	// decisions that let a writer make progress right away.
	DiskFull bool

	// Attempt counts how many times this particular write has been reconsidered
	// after blocking, so a policy can block for a while and then give up.
	Attempt int
}

// Policy decides what happens to a write that does not fit.
type Policy interface {
	OnFull(ctx context.Context, s State) Decision
}

// PolicyFunc adapts a function to the Policy interface.
type PolicyFunc func(ctx context.Context, s State) Decision

func (f PolicyFunc) OnFull(ctx context.Context, s State) Decision { return f(ctx, s) }

func constant(d Decision) Policy {
	return PolicyFunc(func(context.Context, State) Decision { return d })
}

// Block makes writers wait for the sink to catch up. This is the default: it
// propagates backpressure to whatever is producing the events, which is usually
// the only thing that can actually slow down.
func Block() Policy { return constant(DecisionBlock) }

// Reject fails writes with ErrFull once the buffer is full.
func Reject() Policy { return constant(DecisionReject) }

// DropNewest discards incoming writes once the buffer is full.
func DropNewest() Policy { return constant(DecisionDropNewest) }

// DropOldest discards the oldest unflushed records to admit new ones.
func DropOldest() Policy { return constant(DecisionDropOldest) }

// BlockThenDropOldest waits for space for up to the given duration and then
// starts shedding the backlog. It is a reasonable default for telemetry: a
// brief sink hiccup applies backpressure, a sustained outage does not take the
// application down with it.
func BlockThenDropOldest(grace time.Duration) Policy {
	return PolicyFunc(func(_ context.Context, s State) Decision {
		if s.OldestAge > grace {
			return DecisionDropOldest
		}
		return DecisionBlock
	})
}
