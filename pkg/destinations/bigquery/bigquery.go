package bigquery

import (
	"context"
	"github.com/scratchdata/scratchdata/util"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type BigQueryServer struct {
	CredentialsJsonString string `mapstructure:"credentials_json"`
	Location              string `mapstructure:"location"`
	// this will be implemented during runtime

	GCSBucketName string `mapstructure:"gcs_bucket_name"`
	GCSFilePrefix string `mapstructure:"gcs_file_prefix"`

	DeleteFromGCS bool `mapstructure:"delete_from_gcs"`

	Credentials *google.Credentials
	conn        *bigquery.Client
}

func openConn(s *BigQueryServer) (*bigquery.Client, error) {
	ctx := context.Background()

	credentials, err := google.CredentialsFromJSON(ctx, []byte(s.CredentialsJsonString), bigquery.Scope)
	if err != nil {
		log.Error().Err(err).Msg("bigquery credentials error")
		return nil, err
	}

	client, err := bigquery.NewClient(ctx, credentials.ProjectID, option.WithCredentials(credentials))
	if err != nil {
		log.Error().Err(err).Msg("bigquery conn error")
		return nil, err
	}

	s.Credentials = credentials

	log.Info().Msg("Connected to BigQuery")
	return client, nil
}

func OpenServer(settings map[string]interface{}) (*BigQueryServer, error) {
	srv := util.ConfigToStruct[BigQueryServer](settings)

	conn, err := openConn(srv)
	if err != nil {
		log.Error().Err(err).Msg("BigQuery OpenServer Error")
		return nil, err
	}
	srv.conn = conn
	return srv, nil
}

func (s *BigQueryServer) Close() error {
	return s.conn.Close()
}
