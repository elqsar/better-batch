package clickhouse

import (
	"context"
	"errors"
	"testing"

	batch "github.com/elqsar/better-batch"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type event struct {
	ID   uint64
	Name string
}

func eventRow(e event) []any { return []any{e.ID, e.Name} }

// fakeConn hands out fakeBatches and remembers them.
type fakeConn struct {
	prepareErr error
	appendErr  error
	sendErr    error
	queries    []string
	batches    []*fakeBatch
}

func (c *fakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.queries = append(c.queries, query)
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	b := &fakeBatch{appendErr: c.appendErr, sendErr: c.sendErr}
	c.batches = append(c.batches, b)
	return b, nil
}

// fakeBatch embeds the interface so it satisfies it; only the methods the
// sink calls are implemented.
type fakeBatch struct {
	driver.Batch
	appendErr error
	sendErr   error
	rows      [][]any
	sent      bool
	closed    bool
}

func (b *fakeBatch) Append(v ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.rows = append(b.rows, v)
	return nil
}

func (b *fakeBatch) Send() error {
	b.sent = true
	return b.sendErr
}

func (b *fakeBatch) Close() error {
	b.closed = true
	return nil
}

func newSink(t *testing.T, conn Conn, opts ...Option) *Sink[event] {
	t.Helper()
	s, err := New(conn, "db.events", []string{"id", "na`me"}, eventRow, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestFlushInsertsEveryRecord(t *testing.T) {
	conn := &fakeConn{}
	s := newSink(t, conn)
	err := s.Flush(context.Background(), batch.Batch[event]{ID: 7, Records: []event{{1, "a"}, {2, "b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := "INSERT INTO db.events (`id`, `na``me`)"; conn.queries[0] != want {
		t.Errorf("query = %q, want %q", conn.queries[0], want)
	}
	b := conn.batches[0]
	if len(b.rows) != 2 || b.rows[1][1] != "b" || !b.sent || !b.closed {
		t.Errorf("batch = %+v", b)
	}
}

func TestFlushEmptyBatchIsNoop(t *testing.T) {
	conn := &fakeConn{}
	if err := newSink(t, conn).Flush(context.Background(), batch.Batch[event]{ID: 1}); err != nil {
		t.Fatal(err)
	}
	if len(conn.queries) != 0 {
		t.Errorf("prepared %d batches for no records", len(conn.queries))
	}
}

func TestAppendFailureIsPermanent(t *testing.T) {
	conn := &fakeConn{appendErr: errors.New("converting string to UInt64")}
	err := newSink(t, conn).Flush(context.Background(), batch.Batch[event]{ID: 1, Records: []event{{1, "a"}}})
	if !errors.Is(err, batch.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	if !conn.batches[0].closed || conn.batches[0].sent {
		t.Error("an abandoned batch must be closed and not sent")
	}
	if got := Classify(batch.SinkFailure{Err: err}); got.Action != batch.DeadLetterBatch {
		t.Errorf("Classify = %v, want dead_letter", got.Action)
	}
}

func TestRowShapeFailsBuffer(t *testing.T) {
	conn := &fakeConn{}
	s, err := New(conn, "t", []string{"a", "b", "c"}, eventRow)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Flush(context.Background(), batch.Batch[event]{ID: 1, Records: []event{{1, "a"}}})
	if !errors.Is(err, errRowShape) || errors.Is(err, batch.ErrPermanent) {
		t.Fatalf("err = %v, want errRowShape and not permanent", err)
	}
	if got := Classify(batch.SinkFailure{Err: err}); got.Action != batch.FailBuffer {
		t.Errorf("Classify = %v, want fail", got.Action)
	}
}

func TestServerErrors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		prepare   bool
		code      int32
		permanent bool
		want      batch.RetryAction
	}{
		{"type mismatch on send", false, 53, true, batch.DeadLetterBatch},
		{"unknown table on prepare", true, 60, false, batch.FailBuffer},
		{"auth failure on prepare", true, 516, false, batch.FailBuffer},
		{"too many parts on send", false, 252, false, batch.RetryBatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex := &ch.Exception{Code: tc.code, Message: tc.name}
			conn := &fakeConn{}
			if tc.prepare {
				conn.prepareErr = ex
			} else {
				conn.sendErr = ex
			}
			err := newSink(t, conn).Flush(context.Background(), batch.Batch[event]{ID: 1, Records: []event{{1, "a"}}})
			if err == nil {
				t.Fatal("Flush succeeded")
			}
			if got := errors.Is(err, batch.ErrPermanent); got != tc.permanent {
				t.Errorf("permanent = %v, want %v (err %v)", got, tc.permanent, err)
			}
			if got := Classify(batch.SinkFailure{Err: err}); got.Action != tc.want {
				t.Errorf("Classify = %v, want %v", got.Action, tc.want)
			}
		})
	}
}

func TestDeduplicationToken(t *testing.T) {
	s := newSink(t, &fakeConn{}, WithDeduplication("ingest-1"), WithSettings(ch.Settings{"async_insert": 1}))
	got := s.settings(batch.Batch[event]{ID: 10, Records: make([]event, 3)})
	if got["insert_deduplication_token"] != "ingest-1:10-12" || got["async_insert"] != 1 {
		t.Errorf("settings = %v", got)
	}
	if _, leaked := s.cfg.settings["insert_deduplication_token"]; leaked {
		t.Error("token written into the shared settings map")
	}

	plain := newSink(t, &fakeConn{})
	if got := plain.settings(batch.Batch[event]{ID: 1, Records: make([]event, 1)}); got != nil {
		t.Errorf("settings without options = %v, want nil", got)
	}
}

func TestNewRejectsBadArguments(t *testing.T) {
	conn := &fakeConn{}
	for name, f := range map[string]func() error{
		"nil conn":     func() error { _, err := New(nil, "t", []string{"a"}, eventRow); return err },
		"empty table":  func() error { _, err := New(conn, "", []string{"a"}, eventRow); return err },
		"no columns":   func() error { _, err := New(conn, "t", nil, eventRow); return err },
		"empty column": func() error { _, err := New(conn, "t", []string{"a", ""}, eventRow); return err },
		"nil row":      func() error { _, err := New[event](conn, "t", []string{"a"}, nil); return err },
	} {
		if f() == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}
