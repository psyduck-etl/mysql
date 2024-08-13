package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

func flat[T any](c [][]T) []T {
	size := 0
	for _, sub := range c {
		size += len(sub)
	}
	f := make([]T, size)
	ptr := 0
	for _, sub := range c {
		copy(f[ptr:ptr+len(sub)], sub)
		ptr += len(sub)
	}
	return f
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

func queryBlank(fields, chunks int) string {
	return strings.Join(repeat( // "(?,...), " * chunks
		"("+strings.Join(repeat("?", fields), ", ")+")", // "?, " * len(fields)
		chunks), ", ")
}

func queryFormattable(table string, fields []string, chunk int) string {
	return fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
		table, strings.Join(fields, ", "), queryBlank(len(fields), chunk))
}

type Config struct {
	Connection string   `psy:"connection"`
	Table      string   `psy:"table"`
	Fields     []string `psy:"fields"`
	Encoding   string   `psy:"encoding"`

	InsertChunkSize int `psy:"insert-chunk-size"`
}

var specMap = map[string]*sdk.Spec{
	"connection": {
		Name:        "connection",
		Description: "Connection string to a mysql host configured, having a database with the target table",
		Required:    true,
		Type:        cty.String,
	},
	"table": {
		Name:        "table",
		Description: "Table to stick items onto",
		Required:    true,
		Type:        cty.String,
	},
	"fields": {
		Name:        "fields",
		Description: "Fields to extract and stick onto the table",
		Required:    true,
		Type:        cty.List(cty.String),
	},
	"encoding": {
		Name:        "encoding",
		Description: "Encoding that incoming data will be marhsaled with. For now, only JSON is supported",
		Required:    false,
		Type:        cty.String,
		Default:     cty.StringVal("JSON"),
	},
	"insert-chunk-size": {
		Name:        "insert-chunk-size",
		Description: "Number of items to insert at once, with a single query",
		Required:    false,
		Type:        cty.Number,
		Default:     cty.NumberIntVal(1),
	},
}

func Plugin() *sdk.Plugin {
	return &sdk.Plugin{
		Name: "mysql",
		Resources: []*sdk.Resource{
			{
				Kinds: sdk.CONSUMER,
				Name:  "mysql-table",
				Spec:  specMap,
				ProvideConsumer: func(parse sdk.Parser) (sdk.Consumer, error) {
					config := new(Config)
					if err := parse(config); err != nil {
						return nil, err
					}

					if len(config.Fields)*config.InsertChunkSize > 65_535 {
						return nil, fmt.Errorf("too many placeholders, %d ( %d fields * %d ) overflows u16",
							len(config.Fields)*config.InsertChunkSize, len(config.Fields), config.InsertChunkSize)
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
					queryFmt := queryFormattable(config.Table, config.Fields, config.InsertChunkSize)
					return func(recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
						defer close(done)
						defer close(errs)

						for {
							chunk := make([][]any, config.InsertChunkSize)
							for i := 0; i < config.InsertChunkSize; i++ {
								chunk[i] = make([]any, len(config.Fields))
								data := <-recv
								if data == nil {
									if _, err := db.Exec(queryFormattable(config.Table, config.Fields, i)); err != nil {
										errs <- err
									}

									return
								}

								dataDecoded, err := decode(data)
								if err != nil {
									errs <- err
									return
								}

								copy(chunk[i], pickOrdered(config.Fields, dataDecoded))
							}

							if _, err := db.Exec(queryFmt, flat(chunk)); err != nil {
								errs <- err
								return
							}
						}
					}, nil
				},
			},
			{
				Kinds: sdk.TRANSFORMER,
				Name:  "mysql-filter",
				Spec:  specMap,
				ProvideTransformer: func(parse sdk.Parser) (sdk.Transformer, error) {
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
					return func(in []byte) ([]byte, error) {
						dataDecoded, err := decode(in)
						if err != nil {
							return nil, err
						}

						rows, err := db.Query(query, dataDecoded[config.Fields[0]])
						if err != nil {
							return nil, err
						}

						defer rows.Close()
						rows.Next()

						count := -1
						rows.Scan(&count)
						switch count {
						case 1:
							return nil, nil
						case 0:
							return in, nil
						default:
							return nil, fmt.Errorf("count(*) scanned as %d, or did not scan if -1", count)
						}
					}, nil
				},
			},
		},
	}
}
