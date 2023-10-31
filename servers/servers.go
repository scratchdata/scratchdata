package servers

type ClickhouseManager interface {
	GetServers() []ClickhouseServer
}

type ClickhouseServer interface {
	GetHost() string
	GetPort() int
}
