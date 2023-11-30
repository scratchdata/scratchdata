package servers

import "scratchdb/servers/dummy"

func NewDefaultServerManager() DatabaseServerManager {
	return &DefaultServerManager{}

}

type DefaultServerManager struct {
}

func (m *DefaultServerManager) GetServers() []DatabaseServer {
	return []DatabaseServer{dummy.NewDummyDBServer()}

}

func (m *DefaultServerManager) GetServersByAPIKey(apiKey string) []DatabaseServer {
	return []DatabaseServer{dummy.NewDummyDBServer()}
}
