package batch

import (
	"bytes"
	"context"
	"encoding/json"
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
// same ID.
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
