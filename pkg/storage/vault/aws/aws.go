package aws

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/scratchdata/scratchdata/config"
)

type AWSVault struct {
	client *secretsmanager.Client
	prefix string
}

func NewAWSVault(conf map[string]any, destinations []config.Destination) (*AWSVault, error) {
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

	for _, dest := range destinations {
		// Marshal the destination to JSON
		destJSON, err := json.Marshal(dest)
		if err != nil {
			return nil, err
		}

		// Store the JSON string in AWS Secrets Manager
		secretName := prefix + strconv.Itoa(int(dest.ID))
		_, err = vault.client.PutSecretValue(context.Background(), &secretsmanager.PutSecretValueInput{
			SecretId:     aws.String(secretName),
			SecretString: aws.String(string(destJSON)),
		})

		if err != nil {
			return nil, err
		}
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
