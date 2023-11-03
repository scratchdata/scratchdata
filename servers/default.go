package servers

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DefaultServerManager struct {
	servers []DefaultServer
}

func (m *DefaultServerManager) GetServersByDBName(dbName string) []ClickhouseServer {
	panic("not implemented") // TODO: Implement
}

func (m *DefaultServerManager) GetServersByDBCluster(dbCluster string) []ClickhouseServer {
	panic("not implemented") // TODO: Implement
}

func (m *DefaultServerManager) GetServers() []ClickhouseServer {
	rc := []ClickhouseServer{
		&DefaultServer{Host: "1.1.1.1", Port: 10},
		&DefaultServer{Host: "2.2.2.2", Port: 20},
		&DefaultServer{Host: "3.3.3.3", Port: 30},
	}
	return rc
}

type DefaultServer struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (s *DefaultServer) GetHost() string {
	return s.Host
}

func (s *DefaultServer) GetPort() int {
	return s.Port
}

func (s *DefaultServer) GetHttpPort() string {
	panic("not implemented") // TODO: Implement
}

func (s *DefaultServer) GetHttpProtocol() string {
	panic("not implemented") // TODO: Implement
}

func (s *DefaultServer) GetRootUser() string {
	panic("not implemented") // TODO: Implement
}

func (s *DefaultServer) GetRootPassword() string {
	panic("not implemented") // TODO: Implement
}

func (s *DefaultServer) GetStoragePolicy() string {
	panic("not implemented") // TODO: Implement
}

func (s *DefaultServer) Connection() (driver.Conn, error) {
	panic("not implemented") // TODO: Implement
}
