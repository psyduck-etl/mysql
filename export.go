package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/psyduck-etl/sdk"
	"github.com/zclconf/go-cty/cty"

	_ "github.com/go-sql-driver/mysql"
)

func decodeFor(kind string) (func(in []byte) (map[string]interface{}, error), error) {
	switch kind {
	case "JSON":
		return func(in []byte) (map[string]interface{}, error) {
			v := make(map[string]interface{})
			err := json.Unmarshal(in, &v)
			return v, err
		}, nil
	default:
		return nil, fmt.Errorf("no way to decode %s", kind)
	}
}

func repeat[T any](r T, count int) []T {
	ts := make([]T, count)
	for i := 0; i < count; i++ {
		ts[i] = r
	}

	return ts
}

func pickOrdered(fields []string, kvs map[string]any) []any {
	picked := make([]any, len(fields))
	for i, f := range fields {
		if v, ok := kvs[f]; !ok {
			picked[i] = nil
		} else {
			picked[i] = v
		}
	}

	return picked
}

type Config struct {
	Connection string   `psy:"connection"`
	Table      string   `psy:"table"`
	Fields     []string `psy:"fields"`
	Encoding   string   `psy:"encoding"`

	InsertChunkSize int `psy:"insert-chunk-size"`
}

var specs = []*sdk.Spec{
	{
		Name:        "connection",
		Description: "Connection string to a mysql host configured, having a database with the target table",
		Required:    true,
		Type:        cty.String,
	},
	{
		Name:        "table",
		Description: "Table to stick items onto",
		Required:    true,
		Type:        cty.String,
	},
	{
		Name:        "fields",
		Description: "Fields to extract and stick onto the table",
		Required:    true,
		Type:        cty.List(cty.String),
	},
	{
		Name:        "encoding",
		Description: "Encoding that incoming data will be marhsaled with. For now, only JSON is supported",
		Type:        cty.String,
		Default:     cty.StringVal("JSON"),
	},
	{
		Name:        "insert-chunk-size",
		Description: "Number of items to insert at once, with a single query",
		Type:        cty.Number,
		Default:     cty.NumberIntVal(1),
	},
}

// MySQL Table Consumer
type mysqlTableConsumer struct {
	config *Config
	db     *sql.DB
	decode func(in []byte) (map[string]interface{}, error)
	query  string
}

func (c *mysqlTableConsumer) Consume(ctx context.Context, recv func() ([]byte, error)) error {
	defer c.db.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, err := recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			dataDecoded, err := c.decode(data)
			if err != nil {
				return err
			}

			if _, err := c.db.ExecContext(ctx, c.query, pickOrdered(c.config.Fields, dataDecoded)...); err != nil {
				return err
			}
		}
	}
}

func (c *mysqlTableConsumer) Stop() error {
	return c.db.Close()
}

// MySQL Filter Transformer
type mysqlFilterTransformer struct {
	config *Config
	db     *sql.DB
	decode func(in []byte) (map[string]interface{}, error)
	query  string
}

func (t *mysqlFilterTransformer) Transform(in []byte) ([]byte, error) {
	dataDecoded, err := t.decode(in)
	if err != nil {
		return nil, err
	}

	rows, err := t.db.Query(t.query, dataDecoded[t.config.Fields[0]])
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("no rows returned from count query")
	}

	count := -1
	if err := rows.Scan(&count); err != nil {
		return nil, err
	}

	switch count {
	case 1:
		return nil, nil // Filter out (already exists)
	case 0:
		return in, nil // Pass through (doesn't exist)
	default:
		return nil, fmt.Errorf("count(*) scanned as %d, or did not scan if -1", count)
	}
}

// Provider types
type mysqlTableProvider struct{}

func (mysqlTableProvider) ProvideConsumer(parse sdk.Parser) (sdk.Consumer, error) {
	config := new(Config)
	if err := parse(config); err != nil {
		return nil, err
	}

	decode, err := decodeFor(config.Encoding)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", config.Connection)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(30 * time.Second)

	query := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)",
		config.Table, strings.Join(config.Fields, ", "), strings.Join(repeat("?", len(config.Fields)), ", "))

	return &mysqlTableConsumer{
		config: config,
		db:     db,
		decode: decode,
		query:  query,
	}, nil
}

type mysqlFilterProvider struct{}

func (mysqlFilterProvider) ProvideTransformer(parse sdk.Parser) (sdk.Transformer, error) {
	config := new(Config)
	if err := parse(config); err != nil {
		return nil, err
	}

	if len(config.Fields) != 1 {
		return nil, fmt.Errorf("TODO exactly 1 filter field supported for now")
	}

	decode, err := decodeFor(config.Encoding)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", config.Connection)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(30 * time.Second)
	query := fmt.Sprintf("SELECT count(*) FROM %s where %s=?", config.Table, config.Fields[0])

	return &mysqlFilterTransformer{
		config: config,
		db:     db,
		decode: decode,
		query:  query,
	}, nil
}

var MySQLTable = mysqlTableProvider{}
var MySQLFilter = mysqlFilterProvider{}

func main() {
	plugin := &sdk.Plugin{
		Name: "mysql",
		Resources: []*sdk.Resource{
			{
				Name:            "mysql-table",
				Kinds:           sdk.CONSUMER,
				ProvideConsumer: MySQLTable,
				Spec:            specs,
			},
			{
				Name:               "mysql-filter",
				Kinds:              sdk.TRANSFORMER,
				ProvideTransformer: MySQLFilter,
				Spec:               specs,
			},
		},
	}

	// Run as gRPC client process
	sdk.RunAsClientProcess(plugin)
}
