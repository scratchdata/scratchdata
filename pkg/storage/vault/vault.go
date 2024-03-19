package vault

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/memory"
)

// Vault defines the interface for managing credentials.
type Vault interface {
	GetCredential(name string) (config.Destination, error)
	SetCredential(name string, value config.Destination) error
}

func NewVault(vaultConf config.Vault, destinations []config.Destination) (Vault, error) {
	switch vaultConf.Type {
	case "memory":
		return memory.NewMemoryVault(destinations)
	}

	return nil, nil
}
