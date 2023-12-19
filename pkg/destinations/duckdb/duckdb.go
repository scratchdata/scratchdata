package duckdb

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBServer struct {
	Database string `mapstructure:"database"`
	Token    string `mapstructure:"token"`

	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Bucket          string `mapstructure:"bucket"`
	Region          string `mapstructure:"region"`
	S3Prefix        string `mapstructure:"s3_prefix"`
	Endpoint        string `mapstructure:"endpoint"`
}

var jsonToDuck = map[string]string{
	"string": "STRING",
	"int":    "BIGINT",
	"float":  "DOUBLE",
	"bool":   "BOOLEAN",
}

func (s *DuckDBServer) getConnector() (driver.Connector, error) {

	connector, err := duckdb.NewConnector("md:"+s.Database+"?motherduck_token="+s.Token, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			"INSTALL 'json'",
			"LOAD 'json'",
			"INSTALL 'aws'",
			"LOAD 'aws'",
			"INSTALL 'httpfs'",
			"LOAD 'httpfs'",
		}

		for _, qry := range bootQueries {
			_, err := execer.ExecContext(context.TODO(), qry, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return connector, err
}
