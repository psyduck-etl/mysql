package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/psyduck-etl/sdk"
	"github.com/psyduck-etl/sdk/data"
)

// acceptConfig is a backward-compatibility alias for data.InputCodec.
// See data.InputCodec for the canonical documentation and usage examples.
type acceptConfig = data.InputCodec

// emitConfig is a backward-compatibility alias for data.OutputCodec.
// See data.OutputCodec for the canonical documentation and usage examples.
type emitConfig = data.OutputCodec

// sendErr forwards err onto errs, giving up if ctx is cancelled first.
// Callers use this instead of a bare `errs <- err` to avoid blocking
// indefinitely on an errs channel the host has stopped reading after
// cancellation. Reports whether the error was delivered — false means
// the caller should return without retrying.
func sendErr(ctx context.Context, errs chan<- error, err error) bool {
	select {
	case errs <- err:
		return true
	case <-ctx.Done():
		return false
	}
}

// decoder turns a raw record into a field->value map.
type decoder func(in []byte) (map[string]any, error)

// execer is the subset of *sql.DB used to run the batched inserts. It lets
// the batching loop be exercised in tests against a fake.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// encoder turns a field->value map into a raw record.
type encoder func(v map[string]any) ([]byte, error)

// codecFor resolves an encoding spec via the sdk-registered codec factory.
// The host binary (psyduck) installs a factory at startup; standalone
// tests register a stub in TestMain. Spec strings are normalized to
// lowercase so config values like "JSON" (the historical default) keep
// working against the stdlib's lowercase codec names.
func codecFor(spec string) (sdk.Codec, error) {
	return sdk.GetCodec(strings.ToLower(spec))
}

// decodeFor returns a decoder that produces the field->value map shape
// mysql wants. The underlying codec may hand back any native shape; a
// non-object decode is a caller error.
func decodeFor(kind string) (decoder, error) {
	c, err := codecFor(kind)
	if err != nil {
		return nil, err
	}
	return func(in []byte) (map[string]any, error) {
		v, err := c.Decode(in)
		if err != nil {
			return nil, err
		}
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("decode %s: want object, got %T", kind, v)
		}
		return m, nil
	}, nil
}

func encodeFor(kind string) (encoder, error) {
	c, err := codecFor(kind)
	if err != nil {
		return nil, err
	}
	return func(v map[string]any) ([]byte, error) {
		return c.Encode(v)
	}, nil
}

// recordFrom projects a scanned row (columns paired with cells, positionally)
// into a field->value map, normalizing each cell for encoding.
func recordFrom(columns []string, cells []any) map[string]any {
	record := make(map[string]any, len(columns))
	for i, col := range columns {
		record[col] = normalizeCell(cells[i])
	}
	return record
}

// normalizeCell renders a driver-native scan result as a JSON-friendly value.
// The mysql driver hands back many column types as []byte, which
// encoding/json would base64-encode; surface those as strings instead. Other
// types (int64, float64, bool, time.Time, nil) already encode cleanly.
func normalizeCell(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func repeat[T any](r T, count int) []T {
	ts := make([]T, count)
	for i := 0; i < count; i++ {
		ts[i] = r
	}

	return ts
}

// pickOrdered projects kvs onto fields, in order, using nil for absent keys.
func pickOrdered(fields []string, kvs map[string]any) []any {
	picked := make([]any, len(fields))
	for i, f := range fields {
		if v, ok := kvs[f]; ok {
			picked[i] = v
		}
	}

	return picked
}

// Recognized values for Config.WriteMode.
const (
	WRITE_MODE_INSERT        = "insert"
	WRITE_MODE_INSERT_IGNORE = "insert-ignore"
	WRITE_MODE_UPSERT        = "upsert"
	WRITE_MODE_INCREMENT     = "increment"
)

// WriteModeOK validates c.WriteMode against its companion fields, catching
// mistakes at config-bind time rather than at the first failed insert.
// Currently this just checks that increment mode has an increment column;
// future write-mode-specific requirements belong here too.
func (c *Config) WriteModeOK() error {
	if c.WriteMode == WRITE_MODE_INCREMENT && c.IncrementColumn == "" {
		return fmt.Errorf("mysql.table: increment-column is required when write-mode=increment")
	}
	return nil
}

// buildInsert renders a single multi-row INSERT statement covering rowCount
// rows of c.Fields, honoring c.WriteMode. The returned statement expects
// rowCount*len(c.Fields) positional args, row-major.
// For increment mode, c.IncrementColumn specifies which column to increment
// on duplicate key and is required; buildInsert errors if it's empty.
func (c *Config) buildInsert(rowCount int) (string, error) {
	if len(c.Fields) == 0 {
		return "", fmt.Errorf("buildInsert: no fields")
	}
	if rowCount < 1 {
		return "", fmt.Errorf("buildInsert: rowCount must be >= 1, got %d", rowCount)
	}

	verb, suffix := "INSERT INTO", ""
	switch c.WriteMode {
	case "", WRITE_MODE_INSERT:
		// default: fail loudly on a unique-key collision
	case WRITE_MODE_INSERT_IGNORE:
		verb = "INSERT IGNORE INTO"
	case WRITE_MODE_UPSERT:
		verb = "INSERT INTO"
		sets := make([]string, len(c.Fields))
		for i, f := range c.Fields {
			sets[i] = fmt.Sprintf("%s=VALUES(%s)", f, f)
		}
		suffix = " ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
	case WRITE_MODE_INCREMENT:
		if c.IncrementColumn == "" {
			return "", fmt.Errorf("buildInsert: write-mode=increment requires a non-empty increment column")
		}
		verb = "INSERT INTO"
		suffix = fmt.Sprintf(" ON DUPLICATE KEY UPDATE %s=%s+1", c.IncrementColumn, c.IncrementColumn)
	default:
		return "", fmt.Errorf("unknown write-mode %q (want %s|%s|%s|%s)",
			c.WriteMode, WRITE_MODE_INSERT_IGNORE, WRITE_MODE_INSERT, WRITE_MODE_UPSERT, WRITE_MODE_INCREMENT)
	}

	oneRow := "(" + strings.Join(repeat("?", len(c.Fields)), ", ") + ")"
	rows := strings.Join(repeat(oneRow, rowCount), ", ")
	return fmt.Sprintf("%s %s (%s) VALUES %s%s",
		verb, c.Table, strings.Join(c.Fields, ", "), rows, suffix), nil
}

// buildCreateTable renders a CREATE TABLE IF NOT EXISTS from a trusted,
// author-supplied column/constraint body (the text that goes inside the
// parentheses).
func buildCreateTable(table, schema string) (string, error) {
	if strings.TrimSpace(schema) == "" {
		return "", fmt.Errorf("buildCreateTable: empty schema")
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table, schema), nil
}
