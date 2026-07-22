package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/psyduck-etl/sdk"
)

// consumeInto builds a Consumer that decodes incoming records and writes them
// to config.Table in batches of at most InsertChunkSize rows per statement.
//
// Each record is a point-in-time capture of some external resource, appended
// as its own row: re-ingesting the same entity later simply adds another row
// representing it at that later moment.
//
// The Consumer honors ctx: SQL calls use the context, and errors seen while
// ctx is already cancelled are swallowed — the pipeline is winding down and
// the driver's "context canceled" is not what the operator wants to see.
func consumeInto(db *sql.DB, config *Config) sdk.Consumer {
	chunk := config.InsertChunkSize
	if chunk < 1 {
		chunk = 1
	}

	return func(ctx context.Context, recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)

		batch := make([][]any, 0, chunk)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			query, err := config.buildInsert(len(batch))
			if err != nil {
				return err
			}
			args := make([]any, 0, len(batch)*len(config.Fields))
			for _, row := range batch {
				args = append(args, row...)
			}
			_, err = db.ExecContext(ctx, query, args...)
			batch = batch[:0]
			return err
		}

		for data := range recv {
			v, err := config.Decode(data)
			if err != nil {
				if ctx.Err() == nil {
					errs <- err
				}
				return
			}
			decoded, ok := v.(map[string]any)
			if !ok {
				if ctx.Err() == nil {
					errs <- fmt.Errorf("decode: want object, got %T", v)
				}
				return
			}

			batch = append(batch, pickOrdered(config.Fields, decoded))
			if len(batch) >= chunk {
				if err := flush(); err != nil {
					if ctx.Err() == nil {
						errs <- err
					}
					return
				}
			}
		}

		if err := flush(); err != nil && ctx.Err() == nil {
			errs <- err
		}
	}
}
