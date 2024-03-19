package aws

import (
	"context"
	"fmt"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/vault"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

type AWSVault struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure
	Prefix          string `mapstructure:"prefix"`
}

func NewAWSVault(conf config.Vault) *AWSVault {
	return &AWSVault{
		AccessKeyId:     conf.AccessKeyId,
		SecretAccessKey: conf.SecretAccessKey,
		Prefix:          conf.Prefix,
	}
}

func (vault *AWSVault) GetSecret(ctx context.Context, key string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(vault.Prefix + key),
	}

	result, err := client.GetSecretValue(ctx, input)
	if err != nil {
		return "", err
	}

	return *result.SecretString, nil
}

func (vault *AWSVault) SetSecret(ctx context.Context, key string, value string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.CreateSecretInput{
		Name:         aws.String(vault.Prefix + key),
		SecretString: aws.String(value),
	}

	_, err = client.CreateSecret(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (vault *AWSVault) DeleteSecret(ctx context.Context, key string) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.DeleteSecretInput{
		SecretId: aws.String(vault.Prefix + key),
	}

	_, err = client.DeleteSecret(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

func (vault *AWSVault) ListSecrets(ctx context.Context) ([]string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := secretsmanager.NewFromConfig(cfg)

	input := &secretsmanager.ListSecretsInput{}

	result, err := client.ListSecrets(ctx, input)
	if err != nil {
		return nil, err
	}

	var secrets []string
	for _, secret := range result.SecretList {
		secrets = append(secrets, *secret.Name)
	}

	return secrets, nil
}

func (vault *AWSVault) GetSecrets(ctx context.Context) (map[string]string, error) {

	secrets, err := vault.ListSecrets(ctx)
	if err != nil {
		return nil, err
	}

	secretMap := map[string]string{}
	for _, secret := range secrets {
		value, err := vault.GetSecret(ctx, secret)
		if err != nil {
			return nil, err
		}
		secretMap[secret] = value
	}

	return secretMap, nil
}