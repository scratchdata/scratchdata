package chooser

import (
	apikeys "scratchdb/api_keys"
	"scratchdb/servers"
)

type ServerChooser interface {
	ChooseServerForWriting(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
	ChooseServerForReading(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
}
