package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/psyduck-etl/sdk"

	_ "github.com/go-sql-driver/mysql"
)

// Config configures the mysql.table consumer.
type Config struct {
	Connection string   `psy:"connection"`
	Table      string   `psy:"table"`
	Fields     []string `psy:"fields"`
	Encoding   string   `psy:"encoding"`

	InsertChunkSize int    `psy:"insert-chunk-size"`
	WriteMode       string `psy:"write-mode"`
	Schema          string `psy:"schema"`
}

// FilterConfig configures the mysql.filter transformer.
type FilterConfig struct {
	Connection string `psy:"connection"`
	Encoding   string `psy:"encoding"`
	Query      string `psy:"query"`
	PassWhen   string `psy:"pass-when"`
}

func openDB(connection string) (*sql.DB, error) {
	db, err := sql.Open("mysql", connection)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(30 * time.Second)
	return db, nil
}

var (
	specConnection = &sdk.Spec{
		Name:        "connection",
		Description: "Connection string to a mysql host configured, having a database with the target table",
		Required:    true,
		Type:        sdk.TypeString,
	}
	specTable = &sdk.Spec{
		Name:        "table",
		Description: "Table to interact with",
		Required:    true,
		Type:        sdk.TypeString,
	}
	specEncoding = &sdk.Spec{
		Name:        "encoding",
		Description: "Encoding that incoming data will be marshaled with. For now, only JSON is supported",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "JSON",
	}
)

var consumeSpec = []*sdk.Spec{
	specConnection,
	specTable,
	specEncoding,
	{
		Name:        "fields",
		Description: "Fields to extract from each record and write onto the table, in column order",
		Required:    true,
		Type:        sdk.TypeList,
		ElemType:    &sdk.Spec{Type: sdk.TypeString},
	},
	{
		Name:        "insert-chunk-size",
		Description: "Maximum number of records to write per INSERT statement. Records are buffered and flushed in one multi-row insert, then again on close",
		Required:    false,
		Type:        sdk.TypeInt,
		Default:     1,
	},
	{
		Name:        "write-mode",
		Description: "How to write rows: insert (default; fail on a unique-key collision), insert-ignore (silently skip collisions), replace, or upsert (INSERT ... ON DUPLICATE KEY UPDATE)",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "insert",
	},
	{
		Name:        "schema",
		Description: "Optional column/constraint definitions. When set, the plugin runs CREATE TABLE IF NOT EXISTS <table> (<schema>) before consuming, so the table is guaranteed to exist. Trusted, author-supplied config only",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "",
	},
}

var filterSpec = []*sdk.Spec{
	specConnection,
	specEncoding,
	{
		Name:        "query",
		Description: "SQL query returning a single scalar value. Reference incoming record fields as :name placeholders — they are bound as parameters, never interpolated into the SQL text. Trusted, author-supplied config",
		Required:    true,
		Type:        sdk.TypeString,
	},
	{
		Name:        "pass-when",
		Description: "The record passes when the query's scalar result equals this value (compared as text); otherwise it is dropped",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "1",
	},
}

func Plugin() sdk.Plugin {
	return sdk.NewInProc("mysql",
		&sdk.Resource{
			Kinds: sdk.CONSUMER,
			Name:  "table",
			Spec:  consumeSpec,
			ProvideConsumer: func(parse sdk.Parser) (sdk.Consumer, error) {
				config := new(Config)
				if err := parse(config); err != nil {
					return nil, err
				}

				decode, err := decodeFor(config.Encoding)
				if err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				if config.Schema != "" {
					create, err := buildCreateTable(config.Table, config.Schema)
					if err != nil {
						return nil, err
					}
					if _, err := db.Exec(create); err != nil {
						return nil, fmt.Errorf("ensure table %s: %w", config.Table, err)
					}
				}

				return consumeInto(db, config, decode), nil
			},
		},
		&sdk.Resource{
			Kinds: sdk.TRANSFORMER,
			Name:  "filter",
			Spec:  filterSpec,
			ProvideTransformer: func(parse sdk.Parser) (sdk.Transformer, error) {
				config := new(FilterConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				decode, err := decodeFor(config.Encoding)
				if err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				return filterFor(db, config, decode)
			},
		},
	)
}
