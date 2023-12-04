package destinations

import (
	"io"
	"scratchdata/models"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/dummy"
	"scratchdata/util"
)

// func GetDestination(config map[string]interface{}) DatabaseServer {
func GetDestination(config models.DatabaseConnection) DatabaseServer {
	configType := config.Type
	connectionSettings := config.ConnectionSettings

	switch configType {
	case "dummy":
		return util.ConfigToStruct[*dummy.DummyDBServer](connectionSettings)
	case "duckdb":
		return util.ConfigToStruct[*duckdb.DuckDBServer](connectionSettings)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}
