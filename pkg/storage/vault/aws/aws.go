package aws

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type AWSVault struct {
	client *secretsmanager.Client
	prefix string
}

func NewAWSVault(conf map[string]any) (*AWSVault, error) {
	cfg, err := awsConfig.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}

	var accessKey, secretKey, prefix, region string

	if accessKeyVal, ok := conf["AccessKey"].(string); ok {
		accessKey = accessKeyVal
	} else {
		return nil, errors.New("AccessKey not found or not a string")
	}

	if secretKeyVal, ok := conf["SecretKey"].(string); ok {
		secretKey = secretKeyVal
	} else {
		return nil, errors.New("SecretKey not found or not a string")
	}

	if prefixVal, ok := conf["Prefix"].(string); ok {
		prefix = prefixVal
	} else {
		return nil, errors.New("prefix not found or not a string")
	}

	if regionVal, ok := conf["Region"].(string); ok {
		region = regionVal
	} else {
		return nil, errors.New("region not found or not a string")
	}

	if accessKey != "" && secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	}

	cfg.Region = region
	client := secretsmanager.NewFromConfig(cfg)

	vault := &AWSVault{
		client: client,
		prefix: prefix,
	}

	return vault, nil
}

func (v *AWSVault) GetCredential(name string) (string, error) {
	secretName := v.prefix + name

	req := secretsmanager.GetSecretValueInput{
		SecretId: aws.String(secretName),
	}

	resp, err := v.client.GetSecretValue(context.Background(), &req)
	if err != nil {
		return "", err
	}

	if resp.SecretString == nil {
		return "", errors.New("secret string not found")
	}

	return *resp.SecretString, nil
}

func (v *AWSVault) SetCredential(name, value string) error {
	secretName := v.prefix + name

	_, err := v.client.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(secretName),
		SecretString: aws.String(value),
	})
	if err != nil {
		return err
	}

	return nil
}
