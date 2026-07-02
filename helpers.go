package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// decoder turns a raw record into a field->value map.
type decoder func(in []byte) (map[string]any, error)

// execer is the subset of *sql.DB used to run the batched inserts. It lets
// the batching loop be exercised in tests against a fake.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

func decodeFor(kind string) (decoder, error) {
	switch kind {
	case "JSON":
		return func(in []byte) (map[string]any, error) {
			v := make(map[string]any)
			err := json.Unmarshal(in, &v)
			return v, err
		}, nil
	default:
		return nil, fmt.Errorf("no way to decode %s", kind)
	}
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

// buildInsert renders a single multi-row INSERT statement covering rowCount
// rows of the given fields, honoring the write mode. The returned statement
// expects rowCount*len(fields) positional args, row-major.
func buildInsert(mode, table string, fields []string, rowCount int) (string, error) {
	if len(fields) == 0 {
		return "", fmt.Errorf("buildInsert: no fields")
	}
	if rowCount < 1 {
		return "", fmt.Errorf("buildInsert: rowCount must be >= 1, got %d", rowCount)
	}

	verb, suffix := "INSERT IGNORE INTO", ""
	switch mode {
	case "", "insert-ignore":
		// default: skip rows that collide on a unique key
	case "insert":
		verb = "INSERT INTO"
	case "replace":
		verb = "REPLACE INTO"
	case "upsert":
		verb = "INSERT INTO"
		sets := make([]string, len(fields))
		for i, f := range fields {
			sets[i] = fmt.Sprintf("%s=VALUES(%s)", f, f)
		}
		suffix = " ON DUPLICATE KEY UPDATE " + strings.Join(sets, ", ")
	default:
		return "", fmt.Errorf("unknown write-mode %q (want insert-ignore|insert|replace|upsert)", mode)
	}

	oneRow := "(" + strings.Join(repeat("?", len(fields)), ", ") + ")"
	rows := strings.Join(repeat(oneRow, rowCount), ", ")
	return fmt.Sprintf("%s %s (%s) VALUES %s%s",
		verb, table, strings.Join(fields, ", "), rows, suffix), nil
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

// buildExists renders an existence probe. match fields are ANDed together as
// equality predicates against positional args; filterSQL, when set, is a
// trusted (author-supplied, not record-derived) clause ANDed on top so
// callers can express bounded criteria like "scanned_at > NOW() - INTERVAL 1 HOUR".
func buildExists(table string, fields []string, filterSQL string) (string, error) {
	clauses := make([]string, 0, len(fields)+1)
	for _, f := range fields {
		clauses = append(clauses, fmt.Sprintf("%s=?", f))
	}
	if strings.TrimSpace(filterSQL) != "" {
		clauses = append(clauses, "("+filterSQL+")")
	}
	if len(clauses) == 0 {
		return "", fmt.Errorf("buildExists: need at least one field or a filter-sql clause")
	}
	return fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s WHERE %s)",
		table, strings.Join(clauses, " AND ")), nil
}
