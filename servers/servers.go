package servers

type ClickhouseManager interface {
	GetServers() []ClickhouseServer
	GetServersByDBName(dbName string) []ClickhouseServer
	GetServersByDBCluster(dbCluster string) []ClickhouseServer
}

type ClickhouseServer interface {
	GetHost() string
	GetPort() int
	GetHttpPort() string
	GetHttpProtocol() string
	GetStoragePolicy() string
}
