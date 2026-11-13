package vault

import (
	"errors"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/aws"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/memory"
)

type Vault interface {
	GetCredential(name string) string
	SetCredential(name, value string)
}

func NewVault(conf config.Vault, destinations []config.Destination) (Vault, error) {
	switch conf.Type {
	case "memory":
		return memory.NewVault(destinations)
	case "aws":
		return aws.NewVault(conf.Settings)
	}

	return nil, errors.New("Unable to connect to any vault")
}
