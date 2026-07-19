package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/psyduck-etl/sdk"
)

// filterFor builds a Transformer that runs one SQL query per record and passes
// the record through unchanged when the query's scalar result equals PassWhen
// (compared as text); otherwise it drops the record by returning nil.
//
// The query may reference the incoming record's fields as :name placeholders,
// which are bound as query parameters — record data is never interpolated into
// the SQL text. A single query therefore expresses existence checks,
// de-duplication, recency windows, referential gates, or any other predicate:
//
//	query     = "SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = :order_id)"
//	pass-when = "0"   # keep only orders not already stored
//
// If a grouping fragment is configured (group-n or group-time), filterFor
// returns a batched transformer with double-buffered async flushing. This
// hand-rolled approach avoids loading all records into memory; buffers are
// flushed asynchronously via a worker goroutine while the main loop continues
// accumulating. See github.com/psyduck-etl/sdk#12 (BatchContext feature) —
// this pattern will be replaced by an SDK helper in a future release.
// Otherwise, it returns an unbatched sdk.MapContext transformer.
func filterFor(db *sql.DB, config *FilterConfig) (sdk.Transformer, error) {
	if strings.TrimSpace(config.Query) == "" {
		return nil, fmt.Errorf("mysql.filter requires a query")
	}

	query, names := bindNamed(config.Query)

	// If no grouping is configured, return the fast unbatched path.
	strategy, err := config.groupingConfig.bind()
	if err != nil {
		return nil, err
	}
	if strategy == nil {
		return sdk.MapContext(func(ctx context.Context, in []byte) ([]byte, error) {
			var args []any
			if len(names) > 0 {
				v, err := config.Decode(in)
				if err != nil {
					return nil, err
				}
				decoded, ok := v.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("decode: want object, got %T", v)
				}
				args = make([]any, len(names))
				for i, name := range names {
					args[i] = decoded[name]
				}
			}

			var result any
			if err := db.QueryRowContext(ctx, query, args...).Scan(&result); err != nil {
				return nil, err
			}

			if scalarString(result) == config.PassWhen {
				return in, nil
			}
			return nil, nil
		}), nil
	}

	// Batched path with double-buffered async flushing.
	return func(ctx context.Context, in <-chan []byte, out chan<- []byte, errs chan<- error) {
		defer close(out)

		// bufferChan holds filled buffers for the worker to process.
		// Capacity 1 ensures double-buffering: main loop fills buffer B
		// while worker processes buffer A.
		bufferChan := make(chan [][]byte, 1)

		// workerDone signals when the worker goroutine has exited.
		workerDone := make(chan struct{})

		// Worker goroutine: processes filled buffers and forwards passing
		// records to out. On ctx cancel, exits immediately without draining.
		go func() {
			defer close(workerDone)
			for buffer := range bufferChan {
				for _, msg := range buffer {
					var args []any
					if len(names) > 0 {
						v, err := config.Decode(msg)
						if err != nil {
							select {
							case errs <- err:
							case <-ctx.Done():
								return
							}
							continue
						}
						decoded, ok := v.(map[string]any)
						if !ok {
							select {
							case errs <- fmt.Errorf("decode: want object, got %T", v):
							case <-ctx.Done():
								return
							}
							continue
						}
						args = make([]any, len(names))
						for i, name := range names {
							args[i] = decoded[name]
						}
					}

					var result any
					if err := db.QueryRowContext(ctx, query, args...).Scan(&result); err != nil {
						select {
						case errs <- err:
						case <-ctx.Done():
							return
						}
						continue
					}

					if scalarString(result) == config.PassWhen {
						select {
						case out <- msg:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}()

		// Main loop: accumulates messages into a buffer and flushes according
		// to the strategy.
		var buffer [][]byte
		var timer *time.Timer
		var timerChan <-chan time.Time
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()

		for {
			select {
			case msg, ok := <-in:
				if !ok {
					// Upstream closed. Flush any remaining buffer.
					if len(buffer) > 0 {
						select {
						case bufferChan <- buffer:
						case <-ctx.Done():
						}
					}
					close(bufferChan)
					<-workerDone
					return
				}

				now := time.Now()

				// For time-based flushing, initialize the timer on first message.
				if t, ok := strategy.(*timeFlusher); ok && !t.hasBaseline {
					t.reset(now)
					timer = time.NewTimer(t.window)
					timerChan = timer.C
				}

				buffer = append(buffer, msg)

				// Check if we should flush based on the strategy.
				shouldFlush := strategy.shouldFlush(msg, now, len(buffer))

				if shouldFlush {
					// Flush the buffer via the worker.
					select {
					case bufferChan <- buffer:
						buffer = make([][]byte, 0)
						if t, ok := strategy.(*timeFlusher); ok {
							t.reset(time.Now())
							if timer != nil {
								timer.Stop()
							}
							timer = time.NewTimer(t.window)
							timerChan = timer.C
						} else {
							strategy.reset(time.Now())
						}
					case <-ctx.Done():
						close(bufferChan)
						<-workerDone
						return
					}
				}

			case <-timerChan:
				// Time window has closed. Flush the buffer.
				if len(buffer) > 0 {
					select {
					case bufferChan <- buffer:
						buffer = make([][]byte, 0)
						if t, ok := strategy.(*timeFlusher); ok {
							t.reset(time.Now())
							if timer != nil {
								timer.Stop()
							}
							timer = time.NewTimer(t.window)
							timerChan = timer.C
						}
					case <-ctx.Done():
						close(bufferChan)
						<-workerDone
						return
					}
				} else {
					// Empty buffer but timer fired; restart the timer
					// for the next window.
					if t, ok := strategy.(*timeFlusher); ok {
						if timer != nil {
							timer.Stop()
						}
						timer = time.NewTimer(t.window)
						timerChan = timer.C
					}
				}

			case <-ctx.Done():
				// Context cancelled. Exit without flushing any remaining buffer.
				close(bufferChan)
				<-workerDone
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
