package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Batch is a group of records handed to a sink.
type Batch[T any] struct {
	// ID is the log sequence number of the first record. Records in a batch are
	// consecutive, so Records[i] has the number ID+i, and that number names one
	// record for the life of the buffer: across retries, across restarts, and
	// across a crash that takes the log's tail with it. A sink with an
	// idempotent destination can use it as a deduplication key and get
	// effectively-once delivery.
	//
	// Deduplicate per record rather than per batch. Which records travel
	// together is not stable — a restart can regroup them — so skipping a whole
	// batch whose ID has been seen before can discard records that came with it
	// the second time.
	ID uint64

	// Records are the decoded events, in the order they were written.
	Records []T

	// Attempt counts from 1 and increases when a batch is retried.
	Attempt int
}

// Len reports how many records the batch holds.
func (b Batch[T]) Len() int { return len(b.Records) }

// Sink receives batches. Flush must be safe for concurrent use if the buffer is
// configured with MaxInFlight greater than one.
//
// Returning nil means the records are durably handled downstream and the buffer
// may forget them. Returning an error causes the batch to be retried with the
// same ID, on the ladder set by [WithRetry].
//
// An error that will never succeed should say so, or the buffer keeps asking:
// wrap [ErrPermanent] and the batch goes to the dead-letter sink instead of
// spending its attempts. [RetryAfter] wraps an error with a delay for a
// destination that has said when to come back. A sink whose errors come from
// somebody else's client library is classified from the outside instead, with
// [WithOnSinkError].
type Sink[T any] interface {
	Flush(ctx context.Context, b Batch[T]) error
}

// SinkFunc adapts a function to the Sink interface.
type SinkFunc[T any] func(ctx context.Context, b Batch[T]) error

func (f SinkFunc[T]) Flush(ctx context.Context, b Batch[T]) error { return f(ctx, b) }

// Codec converts records to and from the bytes stored in the log.
//
// Encode appends to dst and returns the extended slice, so an implementation
// can avoid allocating per record.
//
// Decode receives a slice that aliases an internal read buffer and is only
// valid until the next record is read. An implementation that keeps any part of
// src must copy it.
type Codec[T any] interface {
	Encode(dst []byte, v T) ([]byte, error)
	Decode(src []byte) (T, error)
}

// BytesCodec stores []byte records verbatim. Decode copies, because the record
// outlives the read buffer it came from.
type BytesCodec struct{}

func (BytesCodec) Encode(dst []byte, v []byte) ([]byte, error) { return append(dst, v...), nil }
func (BytesCodec) Decode(src []byte) ([]byte, error)           { return bytes.Clone(src), nil }

// StringCodec stores string records verbatim. Decoding converts to string,
// which copies, so records never alias the read buffer.
type StringCodec struct{}

func (StringCodec) Encode(dst []byte, v string) ([]byte, error) { return append(dst, v...), nil }
func (StringCodec) Decode(src []byte) (string, error)           { return string(src), nil }

// JSONCodec stores records as JSON. Convenient rather than fast; prefer a
// generated or hand-written codec on a hot path.
type JSONCodec[T any] struct{}

func (JSONCodec[T]) Encode(dst []byte, v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return dst, err
	}
	return append(dst, b...), nil
}

func (JSONCodec[T]) Decode(src []byte) (T, error) {
	var v T
	err := json.Unmarshal(src, &v)
	return v, err
}

// DecodeFailure describes a record read back from the log that the codec
// refused. It is handed to the WithOnDecodeFailure handler.
type DecodeFailure struct {
	// LSN names the record, the same number Batch.ID counts from.
	LSN uint64

	// Payload is the stored bytes, exactly as Encode produced them. It aliases
	// an internal read buffer and is only valid until the handler returns: copy
	// anything you keep.
	Payload []byte

	// Err is what the codec returned.
	Err error
}

// DecodeAction is what the buffer does with a record that would not decode.
type DecodeAction int

const (
	// DropRecord discards it and carries on, counting ReasonDecode. The default.
	DropRecord DecodeAction = iota

	// StopBuffer halts delivery, leaving the record and everything after it
	// unacknowledged in the log, so a later Open with a codec that understands
	// them can deliver them.
	StopBuffer
)

// SinkFailure describes a batch the sink refused. It is handed to the
// WithOnSinkError handler.
type SinkFailure struct {
	// ID names the batch, the same value as Batch.ID.
	ID      uint64
	Records int
	Bytes   int

	// Attempt counts from 1. The dead-letter sink gets its own count, starting
	// over from 1 once the primary sink's attempts are exhausted.
	Attempt int

	// DeadLetter is true when the dead-letter sink refused the batch rather
	// than the primary one, so a handler can answer the two differently.
	DeadLetter bool

	// Err is what the sink returned.
	Err error
}

// RetryAction is what the buffer does with a batch the sink refused.
type RetryAction int

const (
	// RetryBatch tries again on the configured ladder, up to the attempt limit
	// if there is one. The default, and what every sink error did before there
	// was anything to say otherwise.
	RetryBatch RetryAction = iota

	// DeadLetterBatch gives up on this batch now, without spending the attempts
	// it has left: the destination will not accept it however often it is
	// asked. The batch goes to the dead-letter sink, or is dropped and counted
	// ReasonRetriesExhausted if there is none.
	DeadLetterBatch

	// FailBuffer stops the buffer, leaving this batch and everything after it
	// unacknowledged so a later Open can deliver them. It is for a destination
	// that is wrong rather than a batch that is — bad credentials, a misrouted
	// endpoint — where dead-lettering the backlog one batch at a time would
	// empty it into the quarantine over a fault that a restart fixes.
	FailBuffer
)

// String makes a RetryAction usable directly as a metric label.
func (a RetryAction) String() string {
	switch a {
	case RetryBatch:
		return "retry"
	case DeadLetterBatch:
		return "dead_letter"
	case FailBuffer:
		return "fail"
	default:
		return "unknown"
	}
}

// RetryDecision is a RetryAction with an optional delay before the next
// attempt. Its zero value retries on the configured backoff, which is what
// happens when nothing is configured.
type RetryDecision struct {
	Action RetryAction

	// After replaces the backoff ladder for the next attempt only, for a sink
	// that has been told when to come back: an HTTP Retry-After, a broker's
	// throttle hint. Zero uses the configured ladder. It is ignored unless
	// Action is RetryBatch, and it does not stop the attempt limit counting —
	// a destination that keeps asking for more time still runs out of tries.
	After time.Duration
}

// RetryAfter wraps err with how long to wait before the next attempt, for a
// sink that has been told when to come back. The buffer uses the delay in
// place of its backoff ladder for that one attempt.
//
//	if resp.StatusCode == http.StatusTooManyRequests {
//		return batch.RetryAfter(retryAfter(resp), fmt.Errorf("throttled"))
//	}
//
// A WithOnSinkError handler takes precedence, so a caller who wants to cap or
// ignore what a destination asks for can.
func RetryAfter(d time.Duration, err error) error {
	if err == nil {
		return nil
	}
	return &retryAfterError{d: d, err: err}
}

// retryAfterError carries a sink's requested delay alongside its error. It is
// unexported because the delay is read through errors.As on the concrete type,
// and a caller building one directly would be duplicating RetryAfter.
type retryAfterError struct {
	d   time.Duration
	err error
}

func (e *retryAfterError) Error() string {
	return fmt.Sprintf("%s (retry after %s)", e.err, e.d)
}

func (e *retryAfterError) Unwrap() error { return e.err }

// RetryDelay reports the delay the sink asked for.
func (e *retryAfterError) RetryDelay() time.Duration { return e.d }
