package main

import (
	"database/sql"
	"fmt"

	"github.com/psyduck-etl/sdk"
)

// filterFor builds a Transformer that asks the database a yes/no question
// about each record and either passes the record through unchanged or drops
// it (by returning nil).
//
// The question is "does a row exist in config.Table matching every one of
// config.Fields (equality against the record's values) and satisfying the
// optional, trusted config.FilterSQL clause?".
//
//   - pass-when = "absent" (default): pass records with NO matching row.
//     This is the de-duplication / "have we ingested this already?" case,
//     and also "have we scanned this source recently?" when FilterSQL
//     carries a time bound.
//   - pass-when = "present": pass only records that DO have a matching row
//     (a referential / bounded-membership gate).
func filterFor(db *sql.DB, config *Config, decode decoder) (sdk.Transformer, error) {
	query, err := buildExists(config.Table, config.Fields, config.FilterSQL)
	if err != nil {
		return nil, err
	}

	passWhenPresent := config.PassWhen == "present"
	if config.PassWhen != "" && config.PassWhen != "present" && config.PassWhen != "absent" {
		return nil, fmt.Errorf("unknown pass-when %q (want absent|present)", config.PassWhen)
	}

	return func(in []byte) ([]byte, error) {
		decoded, err := decode(in)
		if err != nil {
			return nil, err
		}

		var exists bool
		if err := db.QueryRow(query, pickOrdered(config.Fields, decoded)...).Scan(&exists); err != nil {
			return nil, err
		}

		if exists == passWhenPresent {
			return in, nil
		}
		return nil, nil
	}, nil
}
