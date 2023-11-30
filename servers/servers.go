package servers

import "io"

type DatabaseServerManager interface {
	GetServers() []DatabaseServer
	GetServersByAPIKey(apiKey string) []DatabaseServer
}

type DatabaseServer interface {
	InsertBatchFromNDJson(input io.Reader) error
	QueryJSON(query string, writer io.Writer) error
}
