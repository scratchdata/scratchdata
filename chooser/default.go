package chooser

import (
	"errors"
	"scratchdb/apikeys"
	"scratchdb/servers"
)

type DefaultChooser struct{}

func (c *DefaultChooser) chooseFirstServer(serverManager servers.DatabaseServerManager, userManager apikeys.APIKeyDetails) (servers.DatabaseServer, error) {
	var eligibleDBServers []servers.DatabaseServer

	// Find server by API key
	eligibleDBServers = serverManager.GetServersByAPIKey(userManager.GetAPIKey())

	// If a server isn't mapped to an API key, then find it by cluster or db name
	// if eligibleDBServers == nil || len(eligibleDBServers) == 0 {
	// 	if userManager.GetDBCluster() != "" {
	// 		eligibleDBServers = serverManager.GetServersByDBCluster(userManager.GetDBCluster())
	// 	} else {
	// 		eligibleDBServers = serverManager.GetServersByDBName(userManager.GetDBName())
	// 	}
	// }

	if eligibleDBServers == nil || len(eligibleDBServers) == 0 {
		return nil, errors.New("Unable to find eligible server to query")
	}

	return eligibleDBServers[0], nil
}

func (c *DefaultChooser) ChooseServerForWriting(serverManager servers.DatabaseServerManager, userManager apikeys.APIKeyDetails) (servers.DatabaseServer, error) {
	return c.chooseFirstServer(serverManager, userManager)
}

func (c *DefaultChooser) ChooseServerForReading(serverManager servers.DatabaseServerManager, userManager apikeys.APIKeyDetails) (servers.DatabaseServer, error) {
	return c.chooseFirstServer(serverManager, userManager)
}
