package aws

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/mitchellh/mapstructure"
)

type AWSVault struct {
	Client          *secretsmanager.Client
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Prefix          string `mapstructure:"prefix"`
	Region          string `mapstructure:"region"`
}

func NewAWSVault(conf map[string]any) (*AWSVault, error) {
	var vault AWSVault

	err := mapstructure.Decode(conf, &vault)
	if err != nil {
		return nil, err
	}

	if vault.AccessKeyId == "" {
		return nil, errors.New("AccessKeyId not found or not a string")
	}

	if vault.SecretAccessKey == "" {
		return nil, errors.New("SecretAccessKey not found or not a string")
	}

	if vault.Prefix == "" {
		return nil, errors.New("prefix not found or not a string")
	}

	if vault.Region == "" {
		return nil, errors.New("region not found or not a string")
	}

	cfg, err := awsConfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	cfg.Credentials = credentials.NewStaticCredentialsProvider(vault.AccessKeyId, vault.SecretAccessKey, "")
	cfg.Region = vault.Region
	client := secretsmanager.NewFromConfig(cfg)
	vault.Client = client

	return &vault, nil
}

func (v *AWSVault) GetCredential(name string) (string, error) {
	secretName := fmt.Sprintf("%s-%s", v.Prefix, name)

	req := secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}

	resp, err := v.Client.GetSecretValue(context.Background(), &req)
	if err != nil {
		return "", err
	}

	if resp.SecretString == nil {
		return "", errors.New("secret string not found")
	}

	return *resp.SecretString, nil
}

func (v *AWSVault) SetCredential(name, value string) error {
	secretName := fmt.Sprintf("%s-%s", v.Prefix, name)

	_, err := v.Client.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	})
	if err == nil {
		_, err = v.Client.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretId:     aws.String(secretName),
			SecretString: aws.String(value),
		})
		if err != nil {
			return err
		}
		return nil
	}

	_, err = v.Client.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         aws.String(secretName),
		SecretString: aws.String(value),
	})
	if err != nil {
		return err
	}
	return nil
}
