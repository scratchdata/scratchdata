package destinations

import (
	"io"
	"scratchdata/models"
	"scratchdata/pkg/destinations/clickhouse"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/dummy"
	"scratchdata/pkg/destinations/ssh"
	"scratchdata/util"
)

func GetDestination(config models.DatabaseConnection) DatabaseServer {
	configType := config.Type
	connectionSettings := config.ConnectionSettings

	switch configType {
	case "clickhouse":
		return util.ConfigToStruct[*clickhouse.ClickhouseServer](connectionSettings)
	case "duckdb":
		return util.ConfigToStruct[*duckdb.DuckDBServer](connectionSettings)
	case "dummy":
		return util.ConfigToStruct[*dummy.DummyDBServer](connectionSettings)
	case "ssh":
		return util.ConfigToStruct[*ssh.SSHServer](connectionSettings)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}
