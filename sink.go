package batch

import (
	"bytes"
	"context"
	"encoding/json"
)

// Batch is a group of records handed to a sink.
type Batch[T any] struct {
	// ID is the log sequence number of the first record. It is stable across
	// retries and across restarts, so a sink with an idempotent destination can
	// use it as a deduplication key and get effectively-once delivery.
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
