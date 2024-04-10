package destinations

import (
	"github.com/scratchdata/scratchdata/pkg/destinations/bigquery"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/destinations/redshift"
)

var ViewConfig = map[string]struct {
	Type    any
	Display string
}{
	"duckdb": {
		Type:    duckdb.DuckDBServer{},
		Display: "DuckDB",
	},
	"redshift": {
		Type:    redshift.RedshiftServer{},
		Display: "Redshift",
	},
	"bigquery": {
		Type:    bigquery.BigQueryServer{},
		Display: "BigQuery",
	},
	"clickhouse": {
		Type:    clickhouse.ClickhouseServer{},
		Display: "Clickhouse",
	},
}
