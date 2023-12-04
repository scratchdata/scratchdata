package destinations

import (
	"io"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/dummy"
	"scratchdata/util"
)

func GetDestination(config map[string]interface{}) DatabaseServer {
	configType := config["type"]

	switch configType {
	case "dummy":
		return util.ConfigToStruct[*dummy.DummyDBServer](config)
	case "duckdb":
		return util.ConfigToStruct[*duckdb.DuckDBServer](config)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}
