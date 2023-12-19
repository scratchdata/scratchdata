package duckdb

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *DuckDBServer) getS3Client() (*s3.S3, error) {
	storageCreds := credentials.NewStaticCredentials(s.AccessKeyId, s.SecretAccessKey, "")
	storageConfig := aws.NewConfig().
		WithRegion(s.Region).
		WithCredentials(storageCreds).
		WithS3ForcePathStyle(true)

	if s.Endpoint != "" {
		storageConfig.WithEndpoint(s.Endpoint)
	}

	session, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	return s3.New(session, storageConfig), nil
}

func (s *DuckDBServer) writeS3File(input io.ReadSeeker, destination string) error {
	_, err := input.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	s3Client, err := s.getS3Client()
	if err != nil {
		return err
	}

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(destination),
		Body:   input,
	})

	return err
}

func (s *DuckDBServer) deleteS3File(key string) error {
	s3Client, err := s.getS3Client()
	if err != nil {
		return err
	}

	_, err = s3Client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	return err
}
