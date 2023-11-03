package chooser

import (
	"scratchdb/apikeys"
	"scratchdb/servers"
)

type ServerChooser interface {
	ChooseServerForWriting(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
	ChooseServerForReading(servers.ClickhouseManager, apikeys.APIKeyDetails) (servers.ClickhouseServer, error)
}
