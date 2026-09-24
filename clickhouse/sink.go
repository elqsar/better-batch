// Package clickhouse is a [batch.Sink] that inserts batches into a ClickHouse
// table through clickhouse-go/v2.
//
// It is a module of its own, so the driver and its dependencies reach only the
// programs that import this package; the core buffer stays free of them.
//
//	conn, err := ch.Open(&ch.Options{Addr: []string{"localhost:9000"}})
//	...
//	sink, err := clickhouse.New(conn, "events", []string{"ts", "user_id", "name"},
//		func(e Event) []any { return []any{e.At, e.UserID, e.Name} },
//		clickhouse.WithDeduplication("events-ingest-1"),
//	)
//	...
//	buf, err := batch.Open[Event](dir, sink, batch.JSONCodec[Event]{},
//		batch.WithOnSinkError(clickhouse.Classify),
//	)
//
// One Flush is one INSERT. A record the driver cannot convert to its column
// type makes the whole batch fail with [batch.ErrPermanent], and so does a
// server-side rejection of the data, so either goes to the dead-letter sink
// rather than being retried. [Classify] adds the other verdict: a destination
// that is wrong rather than a batch — bad credentials, a missing table — stops
// the buffer with the backlog kept.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	batch "github.com/elqsar/better-batch"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Conn is the part of a clickhouse-go connection the sink uses. A driver.Conn
// from ch.Open satisfies it.
type Conn interface {
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

// Option configures a Sink.
type Option func(*config)

type config struct {
	scope    string
	settings ch.Settings
}

// WithDeduplication sets insert_deduplication_token on every insert, so a
// batch retried after an insert that succeeded but whose acknowledgement was
// lost is dropped by the server rather than stored twice.
//
// The token is scope plus the range of log sequence numbers in the batch.
// Those numbers are only unique within one buffer directory, and ClickHouse
// deduplicates per table, so scope must be unique among the buffers writing
// to the table, and must change if a buffer's directory is deleted and
// recreated: its numbering starts over, and a reused token makes the server
// silently discard records it has never seen.
//
// The table must deduplicate inserts: Replicated*MergeTree does by default, a
// plain MergeTree needs non_replicated_deduplication_window set. A batch
// regrouped after a restart has a different range, so it is inserted again;
// deduplication narrows the window for duplicates, it does not close it.
func WithDeduplication(scope string) Option {
	return func(c *config) { c.scope = scope }
}

// WithSettings sends ClickHouse settings with every insert, for example
// async_insert or insert_quorum.
func WithSettings(s ch.Settings) Option {
	return func(c *config) { c.settings = maps.Clone(s) }
}

// Sink inserts batches into one table. It is safe for concurrent use, so it
// works with batch.WithMaxInFlight greater than one.
type Sink[T any] struct {
	conn    Conn
	query   string
	columns int
	row     func(T) []any
	cfg     config
}

// errRowShape marks a row function that returns the wrong number of values.
// That is the program being wrong, not the batch, so Classify stops the buffer
// instead of dead-lettering every batch in turn.
var errRowShape = errors.New("clickhouse: row has the wrong number of values")

// New returns a sink inserting into table. columns names the columns written,
// in order, and row maps a record to one value per column.
//
// table is used as written, so it may be qualified with a database. Column
// names are quoted.
func New[T any](conn Conn, table string, columns []string, row func(T) []any, opts ...Option) (*Sink[T], error) {
	switch {
	case conn == nil:
		return nil, errors.New("clickhouse: nil conn")
	case table == "":
		return nil, errors.New("clickhouse: empty table name")
	case len(columns) == 0:
		return nil, errors.New("clickhouse: no columns")
	case row == nil:
		return nil, errors.New("clickhouse: nil row function")
	}
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		if c == "" {
			return nil, fmt.Errorf("clickhouse: column %d has an empty name", i)
		}
		quoted[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
	}
	return &Sink[T]{
		conn:    conn,
		query:   fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(quoted, ", ")),
		columns: len(columns),
		row:     row,
		cfg:     cfg,
	}, nil
}

// Flush inserts the batch as one INSERT.
func (s *Sink[T]) Flush(ctx context.Context, b batch.Batch[T]) error {
	if len(b.Records) == 0 {
		return nil
	}
	if settings := s.settings(b); settings != nil {
		ctx = ch.Context(ctx, ch.WithSettings(settings))
	}
	ins, err := s.conn.PrepareBatch(ctx, s.query)
	if err != nil {
		return classifyServer(fmt.Errorf("clickhouse: prepare batch %d: %w", b.ID, err))
	}
	defer ins.Close()
	for i, r := range b.Records {
		vals := s.row(r)
		if len(vals) != s.columns {
			return fmt.Errorf("%w: record %d gave %d, want %d", errRowShape, b.ID+uint64(i), len(vals), s.columns)
		}
		if err := ins.Append(vals...); err != nil {
			// The driver converts values on Append, so this is a record that
			// does not fit its column and never will.
			return fmt.Errorf("clickhouse: record %d: %w: %w", b.ID+uint64(i), batch.ErrPermanent, err)
		}
	}
	if err := ins.Send(); err != nil {
		return classifyServer(fmt.Errorf("clickhouse: insert batch %d: %w", b.ID, err))
	}
	return nil
}

// settings returns what to send with the insert for b, or nil for nothing.
// It builds a fresh map each time, since concurrent flushes carry different
// tokens.
func (s *Sink[T]) settings(b batch.Batch[T]) ch.Settings {
	if s.cfg.scope == "" {
		return s.cfg.settings
	}
	m := make(ch.Settings, len(s.cfg.settings)+1)
	maps.Copy(m, s.cfg.settings)
	last := b.ID + uint64(len(b.Records)) - 1
	m["insert_deduplication_token"] = fmt.Sprintf("%s:%d-%d", s.cfg.scope, b.ID, last)
	return m
}
