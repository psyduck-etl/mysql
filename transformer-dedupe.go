package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/psyduck-etl/sdk"
)

// bulkDedupFor builds a transformer that:
// 1. Accumulates N records in a buffer.
// 2. Issues ONE query: SELECT value FROM (VALUES (...)) WHERE value NOT IN (SELECT...)
// 3. Emits only values that weren't in the table (the new ones).
// 4. Drops values that were seen.
//
// This is dramatically faster than per-record queries for large batches.
// For 100k values, network latency savings can be 1000× (100k queries → 1 query).
//
// The query strategy:
//   SELECT value FROM (VALUES (?), (?), ..., (?)) AS new_vals(value)
//   WHERE value NOT IN (SELECT <table-column> FROM <table>)
//
// Configuration:
//   - value-field: field name in each record to dedup on
//   - table: table to check for seen values
//   - table-column: column in table holding the deduplicated values
//   - group-n or group-time: batching strategy (required; group-n includes max-query-size)
func bulkDedupFor(db *sql.DB, config *BulkDedupConfig, decode decoder) (sdk.Transformer, error) {
	if strings.TrimSpace(config.Field) == "" {
		return nil, fmt.Errorf("mysql.bulk-dedup requires field")
	}
	if strings.TrimSpace(config.Table) == "" {
		return nil, fmt.Errorf("mysql.bulk-dedup requires table")
	}
	if strings.TrimSpace(config.TableColumn) == "" {
		return nil, fmt.Errorf("mysql.bulk-dedup requires table-column")
	}

	strategy, err := config.groupingConfig.bind()
	if err != nil {
		return nil, err
	}
	if strategy == nil {
		return nil, fmt.Errorf("mysql.bulk-dedup requires batching (group-n or group-time)")
	}

	// Get max query size from the flusher (if it's a countFlusher with that capability)
	maxBatchSize := 10000 // Default fallback
	if cf, ok := strategy.(*countFlusher); ok && cf.maxQuerySize > 0 {
		maxBatchSize = cf.maxQuerySize
	}

	return func(ctx context.Context, in <-chan []byte, out chan<- []byte, errs chan<- error) {
		defer close(out)

		// bufferChan holds filled buffers for the worker to process.
		// Capacity 1 ensures double-buffering: main loop fills buffer B
		// while worker processes buffer A.
		bufferChan := make(chan [][]byte, 1)

		// workerDone signals when the worker goroutine has exited.
		workerDone := make(chan struct{})

		// Worker goroutine: processes filled buffers and forwards new values.
		// On ctx cancel, exits immediately without draining.
		go func() {
			defer close(workerDone)
			for buffer := range bufferChan {
				// Extract all values from the buffer and map records for later lookup.
				values := make([]any, 0, len(buffer))
				records := make([]map[string]any, len(buffer))

				for i, msg := range buffer {
					decoded, err := decode(msg)
					if err != nil {
						select {
						case errs <- fmt.Errorf("bulk-dedup decode: %w", err):
						case <-ctx.Done():
							return
						}
						continue
					}
					records[i] = decoded

					val, ok := decoded[config.Field]
					if !ok {
						select {
						case errs <- fmt.Errorf("bulk-dedup: record missing field %q", config.Field):
						case <-ctx.Done():
							return
						}
						continue
					}
					values = append(values, val)
				}

				if len(values) == 0 {
					continue
				}

				// Execute the bulk dedup query to get only new values.
				newValues, err := executeBulkDedup(ctx, db, values, config.Table, config.TableColumn)
				if err != nil {
					select {
					case errs <- fmt.Errorf("bulk-dedup query: %w", err):
					case <-ctx.Done():
						return
					}
					continue
				}

				// Build a set for O(1) lookup of new values.
				newSet := make(map[any]bool)
				for _, v := range newValues {
					newSet[v] = true
				}

				// Emit only records whose values are in newSet.
				for i, record := range records {
					if record == nil {
						continue // Skip records that had decode errors
					}
					val := record[config.Field]
					if newSet[val] {
						select {
						case out <- buffer[i]:
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

				// Check if we should flush based on the strategy or max batch size.
				shouldFlush := strategy.shouldFlush(msg, now, len(buffer)) || len(buffer) >= maxBatchSize

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
				// Time window has closed. Flush the buffer if it has any records.
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
					// Empty buffer but timer fired; restart the timer for the next window.
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

// executeBulkDedup queries the database for which values are NOT already in the table.
// Returns only the new (unseen) values.
//
// Query strategy:
//   SELECT value FROM (VALUES (?), (?), ...) AS new_vals(value)
//   WHERE value NOT IN (SELECT <table-column> FROM <table>)
//
// For very large batches (100k+), the query text grows linearly with the number of values.
// The default max-batch-size of 10,000 keeps queries under ~500KB. For larger batches,
// consider the max-batch-size config to split into multiple queries.
func executeBulkDedup(ctx context.Context, db *sql.DB, values []any, table, column string) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Build the VALUES clause: VALUES (?,?,?,...) for N values
	valuesClause := buildValuesClause(len(values))

	// Build the query: SELECT value FROM (VALUES ...) WHERE value NOT IN (SELECT...)
	query := fmt.Sprintf(
		"SELECT value FROM (%s) AS new_vals(value) WHERE value NOT IN (SELECT %s FROM %s)",
		valuesClause,
		column,
		table,
	)

	rows, err := db.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var newValues []any
	for rows.Next() {
		var val any
		if err := rows.Scan(&val); err != nil {
			return nil, err
		}
		newValues = append(newValues, val)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return newValues, nil
}

// buildValuesClause constructs the VALUES (?,?,?,...) clause for N values.
// Example: buildValuesClause(3) returns "VALUES (?,?),(?,?),(?,?)"
func buildValuesClause(count int) string {
	if count <= 0 {
		return ""
	}
	parts := make([]string, count)
	for i := 0; i < count; i++ {
		parts[i] = "(?)"
	}
	return "VALUES " + strings.Join(parts, ", ")
}
