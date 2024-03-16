package bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/scratchdata/scratchdata/util"
)

type BigQueryServer struct {
	ProjectID string `mapstructure:"project_id"`
	Location  string `mapstructure:"location"`
	Database  string `mapstructure:"database"`

	MaxOpenConns        int `mapstructure:"max_open_conns"`
	MaxIdleConns        int `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int `mapstructure:"conn_max_lifetime_secs"`

	client *bigquery.Client
}

func (s *BigQueryServer) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}

func openClient(s *BigQueryServer) (*bigquery.Client, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, s.ProjectID)
	if err != nil {
		return nil, err
	}

	if s.MaxOpenConns > 0 {
		client.SetMaxOpenConnections(s.MaxOpenConns)
	}
	if s.MaxIdleConns > 0 {
		client.SetMaxIdleConnections(s.MaxIdleConns)
	}
	if s.ConnMaxLifetimeSecs > 0 {
		client.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeSecs) * time.Second)
	}

	return client, nil
}

func OpenServer(settings map[string]interface{}) (*BigQueryServer, error) {
	srv := util.ConfigToStruct[BigQueryServer](settings)

	if srv.ProjectID == "" || srv.Location == "" || srv.Database == "" {
		return nil, errors.New("project_id, location, and database must be provided")
	}

	client, err := openClient(srv)
	if err != nil {
		return nil, err
	}
	srv.client = client

	return srv, nil
}
