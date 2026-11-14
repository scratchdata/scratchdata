package vault

import (
	"fmt"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/aws"
	"github.com/scratchdata/scratchdata/pkg/storage/vault/memory"
)

type Vault interface {
	GetCredential(name string) string
	SetCredential(name string, value string) error
	DeleteCredential(name string) error
	GetCredentials(name string) (map[string]any, error)
	SetCredentials(name string, value map[string]any) error
	Store(key string, value []byte) error
	Retrieve(key string) ([]byte, error)
	Delete(key string) error
}

func NewVault(conf config.Vault, destinations []config.Destination) (Vault, error) {
	switch conf.Type {
	case "", "memory":
		return memory.NewVault(conf.Settings, destinations)
	case "aws", "aws_secrets_manager", "secretsmanager":
		return aws.NewVault(conf.Settings)
	default:
		return nil, fmt.Errorf("unknown vault type: %s", conf.Type)
	}
}
