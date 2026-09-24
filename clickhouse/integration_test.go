//go:build integration

// Run against a live server: task ch:up && task ch:test. CLICKHOUSE_ADDR,
// CLICKHOUSE_USER and CLICKHOUSE_PASSWORD override the compose defaults.

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	batch "github.com/elqsar/better-batch"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func connect(t *testing.T, password string) driver.Conn {
	t.Helper()
	conn, err := ch.Open(&ch.Options{
		Addr: []string{env("CLICKHOUSE_ADDR", "127.0.0.1:9000")},
		Auth: ch.Auth{Username: env("CLICKHOUSE_USER", "test"), Password: password},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// table creates a fresh table named after the test. It deduplicates inserts,
// as a Replicated table would, so the token has something to act on.
func table(t *testing.T, conn driver.Conn) string {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("bb_%d", time.Now().UnixNano())
	err := conn.Exec(ctx, "CREATE TABLE "+name+" (id UInt64, name String) ENGINE = MergeTree ORDER BY id"+
		" SETTINGS non_replicated_deduplication_window = 1000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+name) })
	return name
}

func count(t *testing.T, conn driver.Conn, table string) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func password() string { return env("CLICKHOUSE_PASSWORD", "test") }

func TestIntegrationBufferDelivers(t *testing.T) {
	conn := connect(t, password())
	tbl := table(t, conn)
	s, err := New(conn, tbl, []string{"id", "name"}, eventRow)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := batch.Open[event](t.TempDir(), s, batch.JSONCodec[event]{},
		batch.WithFlush(64, 0, 50*time.Millisecond), batch.WithOnSinkError(Classify))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 1000 {
		if err := buf.Write(ctx, event{uint64(i), fmt.Sprint("e", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := buf.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if n := count(t, conn, tbl); n != 1000 {
		t.Errorf("table holds %d rows, want 1000", n)
	}
}

func TestIntegrationDeduplication(t *testing.T) {
	conn := connect(t, password())
	tbl := table(t, conn)
	s, err := New(conn, tbl, []string{"id", "name"}, eventRow, WithDeduplication("it"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := batch.Batch[event]{ID: 100, Records: []event{{1, "a"}, {2, "b"}, {3, "c"}}}
	for range 2 {
		if err := s.Flush(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if n := count(t, conn, tbl); n != 3 {
		t.Errorf("after a retried batch the table holds %d rows, want 3", n)
	}
	// Same first ID, one more record: a regrouped batch. It must not be
	// taken for the one already stored.
	b.Records = append(b.Records, event{4, "d"})
	if err := s.Flush(ctx, b); err != nil {
		t.Fatal(err)
	}
	if n := count(t, conn, tbl); n != 7 {
		t.Errorf("after a regrouped batch the table holds %d rows, want 7", n)
	}
}

func TestIntegrationUnconvertibleValueIsPermanent(t *testing.T) {
	conn := connect(t, password())
	tbl := table(t, conn)
	s, err := New(conn, tbl, []string{"id", "name"}, func(e event) []any { return []any{"not a number", e.Name} })
	if err != nil {
		t.Fatal(err)
	}
	err = s.Flush(context.Background(), batch.Batch[event]{ID: 1, Records: []event{{1, "a"}}})
	if !errors.Is(err, batch.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
	// The connection went back to the pool in a usable state.
	if n := count(t, conn, tbl); n != 0 {
		t.Errorf("table holds %d rows, want 0", n)
	}
}

func TestIntegrationMisdirectedFailsBuffer(t *testing.T) {
	good := connect(t, password())
	s, err := New(good, "no_such_table_bb", []string{"id", "name"}, eventRow)
	if err != nil {
		t.Fatal(err)
	}
	b := batch.Batch[event]{ID: 1, Records: []event{{1, "a"}}}
	err = s.Flush(context.Background(), b)
	if got := Classify(batch.SinkFailure{Err: err}); got.Action != batch.FailBuffer {
		t.Errorf("unknown table: Classify = %v, want fail (err %v)", got.Action, err)
	}

	bad := connect(t, "wrong-"+password())
	s, err = New(bad, "anything", []string{"id", "name"}, eventRow)
	if err != nil {
		t.Fatal(err)
	}
	err = s.Flush(context.Background(), b)
	if got := Classify(batch.SinkFailure{Err: err}); got.Action != batch.FailBuffer {
		t.Errorf("wrong password: Classify = %v, want fail (err %v)", got.Action, err)
	}
}
