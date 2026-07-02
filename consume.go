package main

import (
	"database/sql"

	"github.com/psyduck-etl/sdk"
)

// consumeInto builds a Consumer that decodes incoming records and writes them
// to config.Table in batches of at most InsertChunkSize rows per statement.
//
// Each record is a point-in-time capture of some external resource, appended
// as its own row: re-ingesting the same entity later simply adds another row
// representing it at that later moment.
func consumeInto(db *sql.DB, config *Config, decode decoder) sdk.Consumer {
	chunk := config.InsertChunkSize
	if chunk < 1 {
		chunk = 1
	}

	return func(recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)

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
			_, err = db.Exec(query, args...)
			batch = batch[:0]
			return err
		}

		for data := range recv {
			decoded, err := decode(data)
			if err != nil {
				errs <- err
				return
			}

			batch = append(batch, pickOrdered(config.Fields, decoded))
			if len(batch) >= chunk {
				if err := flush(); err != nil {
					errs <- err
					return
				}
			}
		}

		if err := flush(); err != nil {
			errs <- err
		}
	}
}
