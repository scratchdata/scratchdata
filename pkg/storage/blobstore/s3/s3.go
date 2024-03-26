package s3

import (
	"context"
	"github.com/scratchdata/scratchdata/util"
	"io"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

type Storage struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`

	client     *s3.Client
	downloader *manager.Downloader
}

func (s *Storage) Upload(path string, r io.ReadSeeker) error {
	input := &s3.PutObjectInput{
		Bucket:             aws.String(s.Bucket),
		Key:                aws.String(path),
		Body:               r,
		ContentDisposition: aws.String("attachment"),
	}
	if _, err := s.client.PutObject(context.TODO(), input); err != nil {
		return err
	}
	return nil
}

func (s *Storage) Download(path string, w io.WriterAt) error {
	_, err := s.downloader.Download(context.TODO(), w, &s3.GetObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})

	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) Delete(path string) error {
	_, err := s.client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(path),
	})

	return err
}

// NewStorage returns a new initialized Storage
func NewStorage(c map[string]any) (*Storage, error) {

	q := util.ConfigToStruct[Storage](c)
	appCreds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(q.AccessKeyId, q.SecretAccessKey, ""))

	cfg, _ := config.LoadDefaultConfig(context.TODO())

	var endpoint *string
	if q.Endpoint != "" {
		endpoint = aws.String(q.Endpoint)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = q.Region
		o.Credentials = appCreds
		o.BaseEndpoint = endpoint
	})

	q.client = client
	q.downloader = manager.NewDownloader(q.client)

	return q, nil
}
