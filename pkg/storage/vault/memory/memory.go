package memory

import (
	"github.com/scratchdata/scratchdata/config"
)

func NewMemoryVault(conf config.vault destination []config.Destination, apiKeys []config.APIKey) *MemoryVault {
	rc := MemoryVault{
		conf:         conf,
		destinations: destination,
		apiKeyToDestination: map[string]int64{},
		adminAPIKeys: apiKeys
	}

	for i, destination := range destinations {
		for _, apiKey := range destination.APIKeys {
			rc.apiKeyToDestination[apiKey] = int64(i)
		}
	}

	return &rc
}

func (vault *MemoryVault) GetDestinationCredentials(vaultID int64) (config.Destination, error) {
	return vault.destinations[vaultID], nil
}