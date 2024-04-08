module github.com/psyduck-etl/mysql

go 1.22.1

require (
	github.com/go-sql-driver/mysql v1.8.1
	github.com/psyduck-etl/sdk v0.2.1
	github.com/zclconf/go-cty v1.14.4
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/zclconf/go-cty => github.com/gastrodon/go-cty v1.14.4-1
