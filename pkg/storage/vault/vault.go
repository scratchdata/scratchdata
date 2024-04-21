package vault

import (
	"fmt"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/aws"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/memory"
)

type Vault interface {
	GetCredential(name string) (string, error)
	SetCredential(name, value string) error
	DeleteCredential(name string) error
}

func NewVault(vaultConf config.Vault, destinations []config.Destination) (Vault, error) {
	switch vaultConf.Type {
	case "memory":
		return memory.NewMemoryVault(destinations)
	case "aws":
		return aws.NewAWSVault(vaultConf.Settings)
	default:
		return nil, fmt.Errorf("unknown vault type: %s", vaultConf.Type)
	}
}
