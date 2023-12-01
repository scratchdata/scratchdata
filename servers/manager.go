package servers

import (
	"scratchdb/config"
	"scratchdb/servers/dummy"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
)

// func DecodeConfig[T any](rawConfig map[string]interface{}) T {
func DecodeConfig[T any](rawConfig config.ServerConfig) T {
	var config T
	if err := mapstructure.Decode(rawConfig, &config); err != nil {
		log.Fatal().Msgf("Error decoding config: %v", err)
	}
	return config
}

func NewDefaultServerManager(serverConfigs []config.ServerConfig) DatabaseServerManager {
	// func NewDefaultServerManager(serverConfigs []interface{}) DatabaseServerManager {
	servers := []DatabaseServer{}

	for _, rawConfig := range serverConfigs {
		// var dbConfig config.ServerConfig
		var server DatabaseServer

		log.Print(rawConfig)
		var baseConfig map[string]interface{}
		if err := mapstructure.Decode(rawConfig, &baseConfig); err != nil {
			log.Fatal().Msgf("Error decoding base config: %v", err)
		}

		configType, _ := baseConfig["type"].(string)
		serverId, ok := baseConfig["id"].(string)
		log.Debug().Msg(serverId)
		log.Print(ok)

		switch configType {
		// case "clickhouse":
		// 	var clickhouseConfig config.ClickhouseConfig
		// 	if err := mapstructure.Decode(rawConfig, &clickhouseConfig); err != nil {
		// 		log.Fatal().Msgf("Error decoding MySQL config: %v", err)
		// 	}
		// 	fmt.Printf("MySQL Config: %+v\n", clickhouseConfig)
		// 	serverConfig = clickhouseConfig
		case "dummy":
			dbConfig := DecodeConfig[config.DummyConfig](rawConfig)
			server = dummy.NewDummyDBServer(dbConfig)
		default:
			log.Printf("Unknown config type: %s\n", configType)
		}

		servers = append(servers, server)
	}

	return &DefaultServerManager{
		servers: servers,
	}
}

type DefaultServerManager struct {
	servers []DatabaseServer
}

func (m *DefaultServerManager) GetServers() []DatabaseServer {
	return m.servers
}

func (m *DefaultServerManager) GetServersByAPIKey(apiKey string) []DatabaseServer {
	return m.servers
	// return []DatabaseServer{dummy.NewDummyDBServer()}
}
