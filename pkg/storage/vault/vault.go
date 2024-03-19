package vault

import (
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/vault"
	"io"
)

type Vault interface {
	GetCredentails(name string) string
	SetCredentials(name, value string)
}

func NewVault(conf config.Vault, destinations []config.Destination, adminKeys []config.APIKey) Vault {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryVault(conf, destinations, adminKeys)
	case "aws":
		return aws.NewAWSVault(conf)
	}

	return nil
}