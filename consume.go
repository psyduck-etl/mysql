package main

import (
	"database/sql"

	"github.com/psyduck-etl/sdk"
)

// consumeInto builds a Consumer that decodes incoming records and writes
// them to config.Table in batches of at most InsertChunkSize rows per
// statement.
//
// When Snapshot is set the whole load runs inside a single transaction
// (optionally TRUNCATE-ing the table first), so the table only ever
// reflects a complete, point-in-time load: the transaction commits on a
// clean drain and rolls back on any error. This is the "snapshot-like
// ingestion" mode — a partially written table is never visible.
func consumeInto(db *sql.DB, config *Config, decode decoder) sdk.Consumer {
	chunk := config.InsertChunkSize
	if chunk < 1 {
		chunk = 1
	}

	return func(recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)

		var exec execer = db
		var tx *sql.Tx
		if config.Snapshot {
			var err error
			if tx, err = db.Begin(); err != nil {
				errs <- err
				return
			}
			if config.Truncate {
				if _, err := tx.Exec("TRUNCATE TABLE " + config.Table); err != nil {
					_ = tx.Rollback()
					errs <- err
					return
				}
			}
			exec = tx
		}

		fail := func(err error) {
			if tx != nil {
				_ = tx.Rollback()
			}
			errs <- err
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

		for data := range recv {
			decoded, err := decode(data)
			if err != nil {
				fail(err)
				return
			}

			batch = append(batch, pickOrdered(config.Fields, decoded))
			if len(batch) >= chunk {
				if err := flush(); err != nil {
					fail(err)
					return
				}
			}
		}

		if err := flush(); err != nil {
			fail(err)
			return
		}

		if tx != nil {
			if err := tx.Commit(); err != nil {
				errs <- err
			}
		}
	}
}
