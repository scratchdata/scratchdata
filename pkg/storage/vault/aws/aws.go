package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/scratchdata/scratchdata/pkg/util"
)

type Vault struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Region          string `mapstructure:"region"`
	Prefix          string `mapstructure:"prefix"`

	client *secretsmanager.Client
}

func (v *Vault) prefixedName(name string) string {
	return v.Prefix + name
}

func (v *Vault) GetCredential(name string) string {
	res, err := v.client.GetSecretValue(context.TODO(), &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(v.prefixedName(name)),
	})
	if err != nil || res.SecretString == nil {
		return ""
	}
	return *res.SecretString
}

func (v *Vault) SetCredential(name, value string) {
	_, _ = v.client.PutSecretValue(context.TODO(), &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(v.prefixedName(name)),
		SecretString: aws.String(value),
	})
}

// NewVault returns a new Vault backed by AWS Secrets Manager
func NewVault(c map[string]any) (*Vault, error) {
	v := util.ConfigToStruct[Vault](c)

	appCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(v.AccessKeyId, v.SecretAccessKey, ""))

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	client := secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
		o.Region = v.Region
		o.Credentials = appCreds
	})

	v.client = client

	return v, nil
}
