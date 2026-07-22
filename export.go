package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/psyduck-etl/sdk"
	"github.com/psyduck-etl/sdk/rpc"

	_ "github.com/go-sql-driver/mysql"
)

// main serves the plugin over gRPC to the psyduck host that launched this
// binary as a subprocess.
func main() { rpc.Serve(Plugin()) }

// Config configures the mysql.table consumer.
type Config struct {
	Connection string   `psy:"connection"`
	Table      string   `psy:"table"`
	Fields     []string `psy:"fields"`

	InsertChunkSize int    `psy:"insert-chunk-size"`
	WriteMode       string `psy:"write-mode"`
	IncrementColumn string `psy:"increment-column"`
	Schema          string `psy:"schema"`
	acceptConfig
}

// FilterConfig configures the mysql.filter transformer.
type FilterConfig struct {
	Connection string `psy:"connection"`
	Query      string `psy:"query"`
	PassWhen   string `psy:"pass-when"`
	acceptConfig
	groupingConfig
}

// BulkDedupConfig configures the mysql.bulk-dedup transformer.
type BulkDedupConfig struct {
	Connection  string `psy:"connection"`
	Field       string `psy:"field"`
	Table       string `psy:"table"`
	TableColumn string `psy:"table-column"`
	acceptConfig
	groupingConfig
}

// QueryConfig configures the mysql.query producer.
type QueryConfig struct {
	Connection string `psy:"connection"`
	Query      string `psy:"query"`
	emitConfig
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
	specAccept = &sdk.Spec{
		Name:        "accept",
		Description: "Codec spec used to decode record bytes. Resolved via the host's registered codec factory — psyduck's stdlib accepts names like \"json\" and \"csv\" as well as chains like \"gzip|json\"",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "json",
	}
	specEmit = &sdk.Spec{
		Name:        "emit",
		Description: "Codec spec used to encode record bytes. Resolved via the host's registered codec factory — psyduck's stdlib accepts names like \"json\" and \"csv\" as well as chains like \"gzip|json\"",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     "json",
	}
)

var consumeSpec = []*sdk.Spec{
	specConnection,
	specTable,
	specAccept,
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
		Description: "How to write rows: insert (default; fail on a unique-key collision), insert-ignore (silently skip collisions), upsert (INSERT ... ON DUPLICATE KEY UPDATE), or increment (INSERT ... ON DUPLICATE KEY UPDATE col = col + 1; requires increment-column)",
		Required:    false,
		Type:        sdk.TypeString,
		Default:     WRITE_MODE_INSERT,
	},
	{
		Name:        "increment-column",
		Description: "Column name to increment on duplicate key collision. Required when write-mode=increment; ignored otherwise",
		Required:    false,
		Type:        sdk.TypeString,
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
	specAccept,
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

var bulkDedupSpec = []*sdk.Spec{
	specConnection,
	specAccept,
	{
		Name:        "field",
		Description: "Field name in each record that holds the value to dedup on",
		Required:    true,
		Type:        sdk.TypeString,
	},
	{
		Name:        "table",
		Description: "Table to check for seen values",
		Required:    true,
		Type:        sdk.TypeString,
	},
	{
		Name:        "table-column",
		Description: "Column in the table holding the deduplicated values",
		Required:    true,
		Type:        sdk.TypeString,
	},
}

func init() {
	// Append grouping specs to filterSpec and bulkDedupSpec at runtime so we can reference
	// groupingSpec() without circular dependencies.
	filterSpec = append(filterSpec, groupingSpec()...)
	bulkDedupSpec = append(bulkDedupSpec, groupingSpec()...)
}

var querySpec = []*sdk.Spec{
	specConnection,
	specEmit,
	{
		Name:        "query",
		Description: "SQL query to run once at startup; each result row is emitted as one record with columns mapped to fields. Parameterize via HCL value/env interpolation, not runtime binding. Trusted, author-supplied config",
		Required:    true,
		Type:        sdk.TypeString,
	},
}

func Plugin() sdk.Plugin {
	return sdk.NewInProc("mysql",
		&sdk.Resource{
			Kinds: sdk.CONSUMER,
			Name:  "table",
			Spec:  consumeSpec,
			ProvideConsumer: func(ctx context.Context, parse sdk.Parser) (sdk.Consumer, error) {
				config := new(Config)
				if err := parse(config); err != nil {
					return nil, err
				}

				if err := config.validate(); err != nil {
					return nil, err
				}

				if err := config.acceptConfig.Bind(); err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				if config.Schema != "" {
					create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", config.Table, config.Schema)
					// Use ctx to allow schema bootstrap to be cancelled if bind times out.
					if _, err := db.ExecContext(ctx, create); err != nil {
						return nil, fmt.Errorf("ensure table %s: %w", config.Table, err)
					}
				}

				return consumeInto(db, config), nil
			},
		},
		&sdk.Resource{
			Kinds: sdk.TRANSFORMER,
			Name:  "filter",
			Spec:  filterSpec,
			ProvideTransformer: func(ctx context.Context, parse sdk.Parser) (sdk.Transformer, error) {
				config := new(FilterConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				if err := config.acceptConfig.Bind(); err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				return filterFor(db, config)
			},
		},
		&sdk.Resource{
			Kinds: sdk.TRANSFORMER,
			Name:  "bulk-dedup",
			Spec:  bulkDedupSpec,
			ProvideTransformer: func(ctx context.Context, parse sdk.Parser) (sdk.Transformer, error) {
				config := new(BulkDedupConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				if err := config.acceptConfig.Bind(); err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				return bulkDedupFor(db, config)
			},
		},
		&sdk.Resource{
			Kinds: sdk.PRODUCER,
			Name:  "query",
			Spec:  querySpec,
			ProvideProducer: func(ctx context.Context, parse sdk.Parser) (sdk.Producer, error) {
				config := new(QueryConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				if err := config.emitConfig.Bind(); err != nil {
					return nil, err
				}

				db, err := openDB(config.Connection)
				if err != nil {
					return nil, err
				}

				return produceQuery(db, config), nil
			},
		},
	)
}
