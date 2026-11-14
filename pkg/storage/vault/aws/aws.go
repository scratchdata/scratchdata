package aws

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"
)

type SecretsManagerAPI interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	PutSecretValue(ctx context.Context, params *secretsmanager.PutSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error)
	DeleteSecret(ctx context.Context, params *secretsmanager.DeleteSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error)
}

type Vault struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
	Prefix          string `mapstructure:"prefix"`

	client SecretsManagerAPI
}

func NewVault(c map[string]any) (*Vault, error) {
	v := util.ConfigToStruct[Vault](c)

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	var endpoint *string
	if v.Endpoint != "" {
		endpoint = aws.String(v.Endpoint)
	}

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		if v.Region != "" {
			o.Region = v.Region
		}
		if v.AccessKeyId != "" || v.SecretAccessKey != "" {
			o.Credentials = aws.NewCredentialsCache(
				credentials.NewStaticCredentialsProvider(v.AccessKeyId, v.SecretAccessKey, ""),
			)
		}
		if endpoint != nil {
			o.BaseEndpoint = endpoint
		}
	})

	v.client = client
	return v, nil
}

func NewVaultWithClient(prefix string, client SecretsManagerAPI) *Vault {
	return &Vault{
		Prefix: prefix,
		client: client,
	}
}

func (v *Vault) secretId(name string) string {
	if v.Prefix == "" {
		return name
	}
	prefix := strings.TrimSuffix(v.Prefix, "/")
	cleanName := strings.TrimPrefix(name, "/")
	return prefix + "/" + cleanName
}

func (v *Vault) GetCredential(name string) string {
	data, err := v.Retrieve(name)
	if err != nil {
		log.Debug().Err(err).Str("secret", name).Msg("Unable to retrieve credential from AWS Secrets Manager")
		return ""
	}
	return string(data)
}

func (v *Vault) SetCredential(name string, value string) error {
	return v.Store(name, []byte(value))
}

func (v *Vault) DeleteCredential(name string) error {
	return v.Delete(name)
}

func (v *Vault) Retrieve(key string) ([]byte, error) {
	id := v.secretId(key)
	out, err := v.client.GetSecretValue(context.TODO(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(id),
	})
	if err != nil {
		return nil, err
	}
	if out.SecretString != nil {
		return []byte(*out.SecretString), nil
	}
	return out.SecretBinary, nil
}

func (v *Vault) Store(key string, value []byte) error {
	id := v.secretId(key)
	secretStr := string(value)

	_, err := v.client.CreateSecret(context.TODO(), &secretsmanager.CreateSecretInput{
		Name:         aws.String(id),
		SecretString: aws.String(secretStr),
	})
	if err != nil {
		var existsErr *types.ResourceExistsException
		if errors.As(err, &existsErr) || strings.Contains(err.Error(), "ResourceExistsException") {
			_, putErr := v.client.PutSecretValue(context.TODO(), &secretsmanager.PutSecretValueInput{
				SecretId:     aws.String(id),
				SecretString: aws.String(secretStr),
			})
			return putErr
		}
		return err
	}
	return nil
}

func (v *Vault) Delete(key string) error {
	id := v.secretId(key)
	_, err := v.client.DeleteSecret(context.TODO(), &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(id),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	return err
}

func (v *Vault) GetCredentials(name string) (map[string]any, error) {
	raw, err := v.Retrieve(name)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (v *Vault) SetCredentials(name string, value map[string]any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return v.Store(name, raw)
}
