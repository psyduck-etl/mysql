package main

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestBuildInsert(t *testing.T) {
	cases := []struct {
		name     string
		mode     string
		rowCount int
		want     string
		wantErr  bool
	}{
		{
			name: "default is plain insert, single row",
			mode: "", rowCount: 1,
			want: "INSERT INTO t (a, b) VALUES (?, ?)",
		},
		{
			name: "insert-ignore, multi-row",
			mode: "insert-ignore", rowCount: 3,
			want: "INSERT IGNORE INTO t (a, b) VALUES (?, ?), (?, ?), (?, ?)",
		},
		{
			name: "replace",
			mode: "replace", rowCount: 1,
			want: "REPLACE INTO t (a, b) VALUES (?, ?)",
		},
		{
			name: "upsert",
			mode: "upsert", rowCount: 2,
			want: "INSERT INTO t (a, b) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE a=VALUES(a), b=VALUES(b)",
		},
		{
			name: "unknown mode errors",
			mode: "nope", rowCount: 1,
			wantErr: true,
		},
		{
			name: "zero rows errors",
			mode: "", rowCount: 0,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildInsert(c.mode, "t", []string{"a", "b"}, c.rowCount)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("buildInsert =\n  %q\nwant\n  %q", got, c.want)
			}
		})
	}
}

func TestBindNamed(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		want      string
		wantNames []string
	}{
		{
			name:      "no placeholders",
			query:     "SELECT 1",
			want:      "SELECT 1",
			wantNames: nil,
		},
		{
			name:      "single placeholder",
			query:     "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)",
			want:      "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = ?)",
			wantNames: []string{"order_id"},
		},
		{
			name:      "multiple placeholders in order",
			query:     "SELECT :a + :b",
			want:      "SELECT ? + ?",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "colon inside a string literal is left alone",
			query:     "SELECT :src WHERE t = '12:00'",
			want:      "SELECT ? WHERE t = '12:00'",
			wantNames: []string{"src"},
		},
		{
			name:      "lone colon is literal",
			query:     "SELECT 1::2",
			want:      "SELECT 1::2",
			wantNames: nil,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, names := bindNamed(c.query)
			if got != c.want {
				t.Fatalf("bindNamed query =\n  %q\nwant\n  %q", got, c.want)
			}
			if !reflect.DeepEqual(names, c.wantNames) {
				t.Fatalf("bindNamed names = %v, want %v", names, c.wantNames)
			}
		})
	}
}

func TestScalarString(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{[]byte("1"), "1"},
		{"present", "present"},
		{int64(0), "0"},
		{int64(42), "42"},
		{true, "true"},
	}
	for _, c := range cases {
		if got := scalarString(c.in); got != c.want {
			t.Fatalf("scalarString(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBuildCreateTable(t *testing.T) {
	got, err := buildCreateTable("captures", "id BIGINT PRIMARY KEY AUTO_INCREMENT, post_id BIGINT, body TEXT, captured_at TIMESTAMP")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "CREATE TABLE IF NOT EXISTS captures (id BIGINT PRIMARY KEY AUTO_INCREMENT, post_id BIGINT, body TEXT, captured_at TIMESTAMP)"
	if got != want {
		t.Fatalf("buildCreateTable =\n  %q\nwant\n  %q", got, want)
	}

	if _, err := buildCreateTable("t", "   "); err == nil {
		t.Fatal("expected error for empty schema")
	}
}

func TestPickOrdered(t *testing.T) {
	got := pickOrdered([]string{"a", "b", "c"}, map[string]any{"a": 1, "c": 3})
	want := []any{1, nil, 3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pickOrdered = %v, want %v", got, want)
	}
}

// recordExecer captures the queries and args a consumer would send, standing
// in for *sql.DB so the batching logic can be exercised without a database.
type recordExecer struct {
	queries []string
	args    [][]any
}

func (r *recordExecer) Exec(query string, args ...any) (sql.Result, error) {
	r.queries = append(r.queries, query)
	r.args = append(r.args, args)
	return driverResult{}, nil
}

type driverResult struct{}

func (driverResult) LastInsertId() (int64, error) { return 0, nil }
func (driverResult) RowsAffected() (int64, error) { return 0, nil }

// flushBatches mirrors the accumulate/flush loop in consumeInto so the
// chunking behaviour can be asserted in isolation.
func flushBatches(exec execer, config *Config, records []map[string]any) error {
	chunk := config.InsertChunkSize
	if chunk < 1 {
		chunk = 1
	}
	batch := make([][]any, 0, chunk)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		query, err := buildInsert(config.WriteMode, config.Table, config.Fields, len(batch))
		if err != nil {
			return err
		}
		args := make([]any, 0, len(batch)*len(config.Fields))
		for _, row := range batch {
			args = append(args, row...)
		}
		_, err = exec.Exec(query, args...)
		batch = batch[:0]
		return err
	}
	for _, rec := range records {
		batch = append(batch, pickOrdered(config.Fields, rec))
		if len(batch) >= chunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func TestBatchedFlush(t *testing.T) {
	config := &Config{Table: "t", Fields: []string{"a", "b"}, InsertChunkSize: 2}
	records := []map[string]any{
		{"a": 1, "b": 2},
		{"a": 3, "b": 4},
		{"a": 5, "b": 6},
	}

	rec := &recordExecer{}
	if err := flushBatches(rec, config, records); err != nil {
		t.Fatalf("flushBatches: %v", err)
	}

	// chunk size 2 over 3 records -> a full batch of 2, then a remainder of 1
	if len(rec.queries) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(rec.queries), rec.queries)
	}
	if want := "INSERT INTO t (a, b) VALUES (?, ?), (?, ?)"; rec.queries[0] != want {
		t.Fatalf("first query = %q, want %q", rec.queries[0], want)
	}
	if want := []any{1, 2, 3, 4}; !reflect.DeepEqual(rec.args[0], want) {
		t.Fatalf("first args = %v, want %v", rec.args[0], want)
	}
	if want := "INSERT INTO t (a, b) VALUES (?, ?)"; rec.queries[1] != want {
		t.Fatalf("second query = %q, want %q", rec.queries[1], want)
	}
	if want := []any{5, 6}; !reflect.DeepEqual(rec.args[1], want) {
		t.Fatalf("second args = %v, want %v", rec.args[1], want)
	}
}
