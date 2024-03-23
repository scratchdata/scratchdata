package vault

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/memory"
)

type Vault interface {
	GetCredential(name string) (string, error)
	SetCredential(name, value string)
}

func NewVault(vaultConf config.Vault, destinations []config.Destination) (Vault, error) {
	switch vaultConf.Type {
	case "memory":
		return memory.NewMemoryVault(destinations)
	}

	return nil, nil
}
