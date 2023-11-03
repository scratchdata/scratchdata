package servers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

	conn  driver.Conn
	mutex sync.Mutex
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

func (s *DefaultServer) getMaxOpenConns() int {
	return 0
}

func (s *DefaultServer) getMaxIdleConns() int {
	return 0
}

func (s *DefaultServer) getConnMaxLifetimeSecs() int {
	return 0
}

func (s *DefaultServer) Connection() (driver.Conn, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// If the connection hasn't been initialized then create it
	if s.conn == nil {
		var ctx = context.Background()
		var conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", s.GetHost(), s.GetPort())},
			Auth: clickhouse.Auth{
				Username: s.GetRootUser(),
				Password: s.GetRootPassword(),
			},
			Debug:           false,
			MaxOpenConns:    s.getMaxOpenConns(),
			MaxIdleConns:    s.getMaxIdleConns(),
			ConnMaxLifetime: time.Second * time.Duration(s.getConnMaxLifetimeSecs()),
		})

		if err != nil {
			return nil, err
		}

		if err := conn.Ping(ctx); err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
			}
			return nil, err
		}

		s.conn = conn
	}

	return s.conn, nil
}
