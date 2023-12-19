package destinations

import (
	"io"
	"scratchdata/models"
	"scratchdata/pkg/destinations/clickhouse"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/util"
)

func GetDestination(dbConfig models.DatabaseConnection) DatabaseServer {
	configType := dbConfig.Type
	connectionSettings := dbConfig.ConnectionSettings

	switch configType {
	case "duckdb":
		return util.ConfigToStruct[*duckdb.DuckDBServer](connectionSettings)
	case "clickhouse":
		return util.ConfigToStruct[*clickhouse.ClickhouseServer](connectionSettings)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(table string, input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}
