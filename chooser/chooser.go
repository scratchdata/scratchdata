package chooser

import (
	"scratchdb/apikeys"
	"scratchdb/servers"
)

type ServerChooser interface {
	ChooseServerForWriting(servers.DatabaseServerManager, apikeys.APIKeyDetails) (servers.DatabaseServer, error)
	ChooseServerForReading(servers.DatabaseServerManager, apikeys.APIKeyDetails) (servers.DatabaseServer, error)
}
