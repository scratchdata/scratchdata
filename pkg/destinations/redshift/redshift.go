package redshift

import (
	"database/sql"
	"fmt"
	"net/url"
	"path"
	"scratchdata/util"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

type RedshiftServer struct {
	// Endpoint is the AWS Redshift endpoint
	// e.g. [workgroup].[id].[region].redshift-serverless.amazonaws.com:[port]/[database]
	Endpoint string `mapstructure:"endpoint"`

	// Schema is the schema to use
	//
	// Default: `public`
	Schema string `mapstructure:"schema"`

	// Database is the database to use
	Database string `mapstructure:"database"`

	// Username is the database username to use
	Username string `mapstructure:"username"`

	// Password is the database password to use
	Password string `mapstructure:"password"`

	// MaxOpenConns see sql.DB.SetMaxOpenConns
	MaxOpenConns int `mapstructure:"max_open_conns"`

	// MaxOpenConns see sql.DB.SetMaxIdleConns
	MaxIdleConns int `mapstructure:"max_idle_conns"`

	// MaxOpenConns see sql.DB.SetConnMaxLifetime
	ConnMaxLifetimeSecs int `mapstructure:"conn_max_lifetime_secs"`

	// TLS if set to false, disables tls, otherwise it's enabled
	TLS *bool `mapstructure:"tls"`

	// InsertBatchSize is the maximum number of messages to insert at once
	InsertBatchSize int `mapstructure:"insert_batch_size"`

	// DatabaseIsPostgres is true, uses queries compatible with a regular Postgres database
	//
	// NOTE: this is only useful for testing
	DatabaseIsPostgres bool `mapstructure:"database_is_postgres"`

	db *sql.DB

	// sqlSchemaDatabasePfx is set, is the quoted database identifier prefix:
	// - if Schema and Database are set: `"schema"."database".`
	// - else if Database are set: `"database".`
	// - else it will be an empty string
	sqlSchemaDatabasePfx string
}

func (s *RedshiftServer) init() error {
	endpoint := s.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	dsn, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("Cannot parse endpoint: %s: %w", endpoint, err)
	}
	qry := dsn.Query()
	if s.TLS != nil && !*s.TLS {
		qry.Set("sslmode", "disable")
	} else {
		qry.Set("sslmode", "require")
	}
	dsn.Scheme = "postgres"
	dsn.User = url.UserPassword(s.Username, s.Password)
	dsn.RawQuery = qry.Encode()

	// if the epoint contains a path, it specifies the database
	// otherwise, take it from the `database` config
	if database := path.Base(dsn.Path); database != "" {
		s.Database = database
	} else {
		dsn.Path = "/" + s.Database
	}

	if s.Schema == "" {
		s.Schema = "public"
	}

	b := &util.StringBuffer{}
	b.SQLIdent(s.Database)
	b.Printf(".")
	b.SQLIdent(s.Schema)
	b.Printf(".")
	s.sqlSchemaDatabasePfx = b.String()

	connector, err := pq.NewConnector(dsn.String())
	if err != nil {
		return fmt.Errorf("Cannot create connector: %s: %w", dsn.Redacted(), err)
	}
	s.db = sql.OpenDB(connector)
	s.db.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeSecs) * time.Second)
	s.db.SetMaxIdleConns(s.MaxIdleConns)
	s.db.SetMaxOpenConns(s.MaxOpenConns)

	err = s.db.Ping()
	log.Debug().
		Err(err).
		Str("dsn", dsn.Redacted()).
		Msg("RedshiftServer: Connect")
	if err != nil {
		s.db.Close()
		return fmt.Errorf("Cannot ping DB: %s: %w", dsn.Redacted(), err)
	}
	return nil
}

func (s *RedshiftServer) Close() error {
	return s.db.Close()
}

func OpenServer(settings map[string]any) (*RedshiftServer, error) {
	srv := util.ConfigToStruct[RedshiftServer](settings)
	if err := srv.init(); err != nil {
		return nil, fmt.Errorf("redshift.OpenServer: %w", err)
	}
	return srv, nil
}
