package servers

type DefaultServerManager struct {
	servers []Server
}

type Server struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func (m *DefaultServerManager) GetServers() []ClickhouseServer {
	rc := []ClickhouseServer{
		&Server{Host: "1.1.1.1", Port: 10},
		&Server{Host: "2.2.2.2", Port: 20},
		&Server{Host: "3.3.3.3", Port: 30},
	}
	return rc
}

func (s *Server) GetHost() string {
	return s.Host
}

func (s *Server) GetPort() int {
	return s.Port
}
