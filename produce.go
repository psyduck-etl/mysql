package main

import (
	"context"
	"database/sql"

	"github.com/psyduck-etl/sdk"
)

// produceQuery builds a Producer that runs one SQL query and emits a record
// per result row, each row encoded as a column->value object. The query runs
// once when the pipeline starts; the producer closes its channels when the
// rows are exhausted or ctx is cancelled.
//
// This is the read counterpart to the table consumer: a pipeline can pull a
// worklist back out of mysql — say a filtered, ordered set of keys to act on
// next — and feed it downstream (into a queue, a lookup, and so on).
func produceQuery(db *sql.DB, config *QueryConfig) sdk.Producer {
	return func(ctx context.Context, send chan<- []byte, errs chan<- error) {
		defer close(send)
		defer close(errs)

		rows, err := db.QueryContext(ctx, config.Query)
		if err != nil {
			if ctx.Err() == nil {
				errs <- err
			}
			return
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			errs <- err
			return
		}

		cells := make([]any, len(columns))
		scan := make([]any, len(columns))
		for i := range cells {
			scan[i] = &cells[i]
		}

		for rows.Next() {
			if err := rows.Scan(scan...); err != nil {
				errs <- err
				return
			}

			data, err := config.Encode(recordFrom(columns, cells))
			if err != nil {
				errs <- err
				return
			}

			select {
			case send <- data:
			case <-ctx.Done():
				return
			}
		}

		if err := rows.Err(); err != nil && ctx.Err() == nil {
			errs <- err
		}
	}
}
