package destinations

import (
	"io"
	"scratchdata/pkg/destinations/duckdb"
	"scratchdata/pkg/destinations/dummy"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
)

// var typeRegistry = map[string]reflect.Type{
// 	"dummy": reflect.TypeOf(dummy.DummyDBServer{}),
// }

// func createDestination(dest string) DatabaseServer {
// 	destinationType, ok := typeRegistry[dest]
// 	if !ok {
// 		return nil
// 	}
// 	v := reflect.New(destinationType).Elem()
// 	return v.Interface().(DatabaseServer)
// }

// func GetDestination(destinationType string, config map[string]string) DatabaseServer {
// 	providerName := config["type"]

// 	var d DatabaseServer = createDestination(providerName)
// 	if d == nil {
// 		return nil
// 	}

// 	err := mapstructure.Decode(config, &d)
// 	if err != nil {
// 		return nil
// 	}

// 	return d
// }

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
	InsertBatchFromNDJson(input io.Reader) error
	QueryJSON(query string, writer io.Writer) error
}
