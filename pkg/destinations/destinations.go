package destinations

import (
	"io"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/dummy"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
)

func ConfigToDestination[T any](rawConfig map[string]interface{}) T {
	var config T
	if err := mapstructure.Decode(rawConfig, &config); err != nil {
		log.Error().Msgf("Error decoding config: %v", err)
	}
	return config
}

func GetDestination(config map[string]interface{}) DatabaseServer {
	configType := config["type"]

	switch configType {
	case "dummy":
		return ConfigToDestination[*dummy.DummyDBServer](config)
	case "duckdb":
		return ConfigToDestination[*duckdb.DuckDBServer](config)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.ReadSeeker) error
	QueryJSON(query string, writer io.Writer) error
}
