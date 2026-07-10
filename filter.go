package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/psyduck-etl/sdk"
)

// filterFor builds a Transformer that runs one SQL query per record and passes
// the record through unchanged when the query's scalar result equals PassWhen
// (compared as text); otherwise the record is dropped (not written to out).
//
// The query may reference the incoming record's fields as :name placeholders,
// which are bound as query parameters — record data is never interpolated into
// the SQL text. A single query therefore expresses existence checks,
// de-duplication, recency windows, referential gates, or any other predicate:
//
//	query     = "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)"
//	pass-when = "0"   # keep only orders not already stored
func filterFor(db *sql.DB, config *FilterConfig, decode decoder) (sdk.Transformer, error) {
	if strings.TrimSpace(config.Query) == "" {
		return nil, fmt.Errorf("mysql.filter requires a query")
	}

	query, names := bindNamed(config.Query)
	// Reused across records — the transformer runs single-threaded per the
	// SDK contract, so we don't re-allocate args on every message.
	var args []any
	if len(names) > 0 {
		args = make([]any, len(names))
	}

	return func(ctx context.Context, in <-chan []byte, out chan<- []byte, errs chan<- error) {
		defer close(out)
		for {
			select {
			case data, ok := <-in:
				if !ok {
					return
				}

				if len(names) > 0 {
					decoded, err := decode(data)
					if err != nil {
						if ctx.Err() == nil {
							select {
							case errs <- err:
							case <-ctx.Done():
								return
							}
						}
						continue
					}
					for i, name := range names {
						args[i] = decoded[name]
					}
				}

				var result any
				if err := db.QueryRowContext(ctx, query, args...).Scan(&result); err != nil {
					if ctx.Err() == nil {
						select {
						case errs <- err:
						case <-ctx.Done():
							return
						}
					}
					continue
				}

				if scalarString(result) != config.PassWhen {
					continue
				}
				select {
				case out <- data:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}, nil
}

// bindNamed rewrites :name placeholders into positional ? markers, returning
// the rewritten query and the placeholder names in order of appearance.
// Colons inside quoted spans ('...', "...", `...`) are left untouched, and a
// colon not followed by an identifier character is treated literally. This is
// a best-effort scanner for trusted, author-supplied queries.
func bindNamed(query string) (string, []string) {
	var out strings.Builder
	var names []string
	runes := []rune(query)
	var quote rune // 0 outside any quoted span

	for i := 0; i < len(runes); i++ {
		c := runes[i]

		if quote != 0 {
			out.WriteRune(c)
			if c == quote {
				quote = 0
			}
			continue
		}

		switch {
		case c == '\'' || c == '"' || c == '`':
			quote = c
			out.WriteRune(c)
		case c == ':' && i+1 < len(runes) && isIdentStart(runes[i+1]):
			j := i + 1
			for j < len(runes) && isIdentPart(runes[j]) {
				j++
			}
			names = append(names, string(runes[i+1:j]))
			out.WriteRune('?')
			i = j - 1
		default:
			out.WriteRune(c)
		}
	}

	return out.String(), names
}

func isIdentStart(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isIdentPart(r rune) bool {
	return isIdentStart(r) || (r >= '0' && r <= '9')
}

// scalarString renders a scanned scalar for text comparison against PassWhen,
// normalizing the driver-native types (int64, float64, []byte, ...) a single
// column can come back as.
func scalarString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return fmt.Sprintf("%v", t)
	}
}
