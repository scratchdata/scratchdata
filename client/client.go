package client

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"scratchdb/config"
)

type Client struct {
	s3  *s3.S3
	sqs *sqs.SQS

	awsConfig     *aws.Config
	storageConfig *aws.Config
}

func NewClient(c *config.Config) *Client {
	client := Client{}

	awsCreds := credentials.NewStaticCredentials(c.AWS.AccessKeyId, c.AWS.SecretAccessKey, "")
	client.awsConfig = aws.NewConfig().
		WithRegion(c.AWS.Region).
		WithEndpoint(c.AWS.Endpoint).
		WithCredentials(awsCreds)

	storageCreds := credentials.NewStaticCredentials(c.Storage.AccessKeyId, c.Storage.SecretAccessKey, "")
	client.storageConfig = aws.NewConfig().
		WithRegion(c.Storage.Region).
		WithEndpoint(c.Storage.Endpoint).
		WithCredentials(storageCreds).
		WithS3ForcePathStyle(true)

	return &client
}

func (c *Client) S3() *s3.S3 {
	if c.s3 != nil {
		return c.s3
	}

	c.s3 = s3.New(session.Must(session.NewSession()), c.storageConfig)
	return c.s3
}

func (c *Client) SQS() *sqs.SQS {
	if c.sqs != nil {
		return c.sqs
	}

	c.sqs = sqs.New(session.Must(session.NewSession()), c.awsConfig)
	return c.sqs
}
