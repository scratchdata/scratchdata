package bigquery

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

type BigQueryServer struct {
	DatasetID             string `mapstructure:"bigquery_dataset_id"`
	CredentialsJsonString string `mapstructure:"bigquery_credentials_json"`

	// this will be implemented during runtime
	ProjectId *string

	GCSBucketName  string `mapstructure:"gcs_bucket_name"`
	GCSAccessKey   *string
	GCSAccessKeyId *string
	GCSFilePrefix  string `mapstructure:"gcs_file_prefix"`

	DeleteFromGCS bool `mapstructure:"delete_from_gcs"`

	Credentials *google.Credentials
	conn        *bigquery.Client
}

func parseCredentials(credentialsString string) (map[string]interface{}, error) {
	var credentials map[string]interface{}
	err := json.Unmarshal([]byte(credentialsString), &credentials)
	if err != nil {
		return nil, err
	}
	return credentials, nil
}

func openConn(s *BigQueryServer) (*bigquery.Client, error) {
	ctx := context.Background()
	credentialsJson, err := parseCredentials(s.CredentialsJsonString)
	if err != nil {
		log.Error().Err(err).Msg("bigquery credentials parsing error")
		return nil, err
	}

	credentials, err := google.CredentialsFromJSON(ctx, []byte(s.CredentialsJsonString), bigquery.Scope)
	if err != nil {
		log.Error().Err(err).Msg("bigquery credentials error")
		return nil, err
	}
	var projectId string
	var ok bool

	projectId, ok = credentialsJson["project_id"].(string)
	if !ok {
		log.Error().Msg("project_id not found in credentials")
		return nil, fmt.Errorf("project_id not found in credentials")

	}

	client, err := bigquery.NewClient(ctx, projectId, option.WithCredentials(credentials))
	if err != nil {
		log.Error().Err(err).Msg("bigquery conn error")
		return nil, err
	}

	s.Credentials = credentials
	s.ProjectId = &projectId

	// at this point it is apparent that credential are present and valid
	privateKeyId, _ := credentialsJson["private_key_id"].(string)
	privateKey, _ := credentialsJson["private_key"].(string)
	s.GCSAccessKeyId = &privateKeyId
	s.GCSAccessKey = &privateKey

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
