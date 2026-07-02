package main

import (
	"database/sql"
	"time"

	"github.com/psyduck-etl/sdk"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Connection string   `psy:"connection"`
	Table      string   `psy:"table"`
	Fields     []string `psy:"fields"`
	Encoding   string   `psy:"encoding"`

	// consumer (mysql-table)
	InsertChunkSize int    `psy:"insert-chunk-size"`
	WriteMode       string `psy:"write-mode"`
	Snapshot        bool   `psy:"snapshot"`
	Truncate        bool   `psy:"truncate"`

	// transformer (mysql-filter)
	PassWhen  string `psy:"pass-when"`
	FilterSQL string `psy:"filter-sql"`
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
		Description: "How to write rows: insert-ignore (skip key collisions), insert (fail on collision), replace, or upsert (INSERT ... ON DUPLICATE KEY UPDATE)",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "insert-ignore",
	},
	{
		Name:        "snapshot",
		Description: "Load every record inside a single transaction, committing only on a clean finish. The table never exposes a partial load",
		Required:    false,
		Type:        sdk.TypeBool,
		Default:     false,
	},
	{
		Name:        "truncate",
		Description: "When snapshot is set, TRUNCATE the table at the start of the transaction so the load fully replaces prior contents",
		Required:    false,
		Type:        sdk.TypeBool,
		Default:     false,
	},
}

var filterSpec = []*sdk.Spec{
	specConnection,
	specTable,
	specEncoding,
	{
		Name:        "fields",
		Description: "Record fields matched by equality against columns of the same name to probe for an existing row",
		Required:    false,
		Type:        sdk.TypeList,
		ElemType:    &sdk.Spec{Type: sdk.TypeString},
	},
	{
		Name:        "pass-when",
		Description: "absent: pass records with no matching row (de-duplication); present: pass only records that have a matching row",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "absent",
	},
	{
		Name:        "filter-sql",
		Description: "Optional trusted SQL predicate ANDed onto the existence probe, e.g. 'scanned_at > NOW() - INTERVAL 1 HOUR'. Author-supplied config only; never interpolate record data here",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "",
	},
}

func Plugin() sdk.Plugin {
	return sdk.NewInProc("mysql",
		&sdk.Resource{
			Kinds: sdk.CONSUMER,
			Name:  "mysql-table",
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

				return consumeInto(db, config, decode), nil
			},
		},
		&sdk.Resource{
			Kinds: sdk.TRANSFORMER,
			Name:  "mysql-filter",
			Spec:  filterSpec,
			ProvideTransformer: func(parse sdk.Parser) (sdk.Transformer, error) {
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

				return filterFor(db, config, decode)
			},
		},
	)
}
