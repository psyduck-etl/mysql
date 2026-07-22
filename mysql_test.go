package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/psyduck-etl/sdk"
)

// TestMain registers a tiny JSON codec factory so mysql tests run without
// a host binary. In production the psyduck host registers the real stdlib
// codec chain; here we just need "json" to work end-to-end for the
// helpers-level tests. Anything else returns an error so the
// unknown-encoding test still passes.
func TestMain(m *testing.M) {
	sdk.RegisterCodecs(func(spec string) (sdk.Codec, error) {
		if spec != "json" {
			return nil, fmt.Errorf("test codec factory: unknown spec %q", spec)
		}
		return jsonCodec{}, nil
	})
	os.Exit(m.Run())
}

type jsonCodec struct{}

func (jsonCodec) Decode(b []byte) (any, error) {
	var v any
	err := json.Unmarshal(b, &v)
	return v, err
}
func (jsonCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }

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
		{
			name: "increment mode without a column errors",
			mode: "increment", rowCount: 1,
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildInsert(c.mode, "t", []string{"a", "b"}, c.rowCount, "")
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

func TestBuildInsertIncrement(t *testing.T) {
	cases := []struct {
		name         string
		rowCount     int
		incrementCol string
		want         string
	}{
		{
			name:         "increment with column n",
			rowCount:     1,
			incrementCol: "n",
			want:         "INSERT INTO t (a, b) VALUES (?, ?) ON DUPLICATE KEY UPDATE n=n+1",
		},
		{
			name:         "increment with custom column name, multi-row",
			rowCount:     2,
			incrementCol: "count",
			want:         "INSERT INTO t (a, b) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE count=count+1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := buildInsert("increment", "t", []string{"a", "b"}, c.rowCount, c.incrementCol)
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

func TestNormalizeCell(t *testing.T) {
	cases := []struct {
		in   any
		want any
	}{
		{[]byte("cats"), "cats"}, // driver []byte -> string, not base64
		{[]byte(nil), ""},        // empty []byte -> empty string
		{int64(42), int64(42)},   // numbers pass through
		{float64(1.5), float64(1.5)},
		{true, true},
		{nil, nil},
	}
	for _, c := range cases {
		if got := normalizeCell(c.in); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("normalizeCell(%#v) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func TestRecordFromEncode(t *testing.T) {
	// A scanned row of driver-native cells (mysql hands text back as []byte)
	// becomes a clean field->value object.
	columns := []string{"tag", "seen"}
	cells := []any{[]byte("cats"), int64(7)}

	encode, err := encodeFor("JSON")
	if err != nil {
		t.Fatalf("encodeFor: %v", err)
	}

	data, err := encode(recordFrom(columns, cells))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	got := make(map[string]any)
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// json numbers decode as float64
	if got["tag"] != "cats" || got["seen"] != float64(7) {
		t.Fatalf("record = %#v, want tag=cats seen=7", got)
	}
}

func TestEncodeForUnknown(t *testing.T) {
	if _, err := encodeFor("XML"); err == nil {
		t.Fatal("expected error for unknown encoding")
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
		query, err := buildInsert(config.WriteMode, config.Table, config.Fields, len(batch), config.IncrementColumn)
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

// Tests for grouping configuration and duration parsing

func TestParseDuration(t *testing.T) {
	cases := []struct {
		input   string
		want    time.Duration
		wantErr bool
	}{
		{"500ms", 500 * time.Millisecond, false},
		{"5s", 5 * time.Second, false},
		{"1m", 1 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"0ms", 0, true},     // zero not allowed
		{"-1s", 0, true},     // negative not allowed
		{"abc", 0, true},     // invalid number
		{"5", 0, true},       // no unit
		{"5x", 0, true},      // invalid unit
		{"", 0, true},        // empty
		{"1ns", 0, true},     // unsupported unit
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%q", c.input), func(t *testing.T) {
			got, err := parseDuration(c.input)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %v", c.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", c.input, err)
			}
			if got != c.want {
				t.Fatalf("parseDuration(%q) = %v, want %v", c.input, got, c.want)
			}
		})
	}
}

func TestGroupingConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		config  *groupingConfig
		wantErr bool
	}{
		{
			name:    "neither group-n nor group-time set is valid (unbatched)",
			config:  &groupingConfig{},
			wantErr: false,
		},
		{
			name: "group-n alone is valid",
			config: func() *groupingConfig {
				c := &groupingConfig{}
				c.GroupN = &struct {
					Size         int `psy:"size"`
					MaxQuerySize int `psy:"max-query-size"`
				}{Size: 10, MaxQuerySize: 5000}
				return c
			}(),
			wantErr: false,
		},
		{
			name: "group-time alone is valid",
			config: &groupingConfig{
				GroupTime: &struct{ Window string }{Window: "5s"},
			},
			wantErr: false,
		},
		{
			name: "both group-n and group-time is an error",
			config: func() *groupingConfig {
				c := &groupingConfig{}
				c.GroupN = &struct {
					Size         int `psy:"size"`
					MaxQuerySize int `psy:"max-query-size"`
				}{Size: 10, MaxQuerySize: 5000}
				c.GroupTime = &struct{ Window string }{Window: "5s"}
				return c
			}(),
			wantErr: true,
		},
		{
			name: "group-n with size <= 0 is an error",
			config: func() *groupingConfig {
				c := &groupingConfig{}
				c.GroupN = &struct {
					Size         int `psy:"size"`
					MaxQuerySize int `psy:"max-query-size"`
				}{Size: 0, MaxQuerySize: 5000}
				return c
			}(),
			wantErr: true,
		},
		{
			name: "group-time with invalid duration is an error",
			config: &groupingConfig{
				GroupTime: &struct{ Window string }{Window: "invalid"},
			},
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := c.config.bind()
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCountFlusher(t *testing.T) {
	f := &countFlusher{size: 3}

	// First two messages shouldn't trigger flush
	if f.shouldFlush([]byte("msg1"), time.Now(), 1) {
		t.Fatal("expected no flush at buf len 1")
	}
	if f.shouldFlush([]byte("msg2"), time.Now(), 2) {
		t.Fatal("expected no flush at buf len 2")
	}

	// Third message hits the size threshold
	if !f.shouldFlush([]byte("msg3"), time.Now(), 3) {
		t.Fatal("expected flush at buf len 3")
	}

	// Reset doesn't affect the threshold
	f.reset(time.Now())
	if !f.shouldFlush([]byte("msg4"), time.Now(), 3) {
		t.Fatal("expected flush after reset at buf len 3")
	}
}

func TestTimeFlusher(t *testing.T) {
	f := &timeFlusher{window: 100 * time.Millisecond}

	now := time.Now()

	// First message within window doesn't trigger flush (no baseline yet)
	if f.shouldFlush([]byte("msg1"), now, 1) {
		t.Fatal("expected no flush before baseline set")
	}

	// Set baseline
	f.reset(now)

	// Message within window shouldn't flush
	if f.shouldFlush([]byte("msg2"), now.Add(50*time.Millisecond), 2) {
		t.Fatal("expected no flush within window")
	}

	// Message after window should flush
	if !f.shouldFlush([]byte("msg3"), now.Add(150*time.Millisecond), 3) {
		t.Fatal("expected flush after window closes")
	}

	// After reset, the baseline moves and timing restarts
	newBaseline := now.Add(150 * time.Millisecond)
	f.reset(newBaseline)

	// Message just within the new window
	if f.shouldFlush([]byte("msg4"), newBaseline.Add(50*time.Millisecond), 1) {
		t.Fatal("expected no flush within new window")
	}
}

// Tests for bulk dedup helpers

func TestBuildValuesClause(t *testing.T) {
	cases := []struct {
		name  string
		count int
		want  string
	}{
		{
			name:  "single value",
			count: 1,
			want:  "VALUES (?)",
		},
		{
			name:  "three values",
			count: 3,
			want:  "VALUES (?), (?), (?)",
		},
		{
			name:  "hundred values",
			count: 100,
			want:  "VALUES " + strings.Repeat("(?), ", 99) + "(?)",
		},
		{
			name:  "zero values",
			count: 0,
			want:  "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := buildValuesClause(c.count)
			if got != c.want {
				t.Fatalf("buildValuesClause(%d) =\n  %q\nwant\n  %q", c.count, got, c.want)
			}
		})
	}
}
