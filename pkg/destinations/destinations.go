package destinations

import (
	"io"
	"scratchdata/pkg/destinations/dummy"

	"github.com/mitchellh/mapstructure"
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

func ConfigToDestination[T any](rawConfig map[string]string) T {
	var config T
	if err := mapstructure.Decode(rawConfig, &config); err != nil {
		// log.Fatal().Msgf("Error decoding config: %v", err)
	}
	return config
}

func GetDestination(config map[string]string) DatabaseServer {
	configType := config["type"]

	switch configType {
	case "dummy":
		return ConfigToDestination[*dummy.DummyDBServer](config)
	default:
		return nil
	}
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.Reader) error
	QueryJSON(query string, writer io.Writer) error
}
