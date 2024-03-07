package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/scratchdata/scratchdata/util"

	"github.com/marcboeker/go-duckdb"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBServer struct {
	Database string `mapstructure:"database"`
	Token    string `mapstructure:"token"`

	File string `mapstructure:"file"`

	InMemory bool `mapstructure:"in_memory"`

	MaxOpenConns        int `mapstructure:"max_open_conns"`
	MaxIdleConns        int `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int `mapstructure:"conn_max_lifetime_secs"`

	db *sql.DB
}

func (s *DuckDBServer) Close() error {
	return s.db.Close()
}

var jsonToDuck = map[string]string{
	"string": "STRING",
	"int":    "BIGINT",
	"float":  "DOUBLE",
	"bool":   "BOOLEAN",
}

func openDB(s *DuckDBServer) (*sql.DB, error) {
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

	db := sql.OpenDB(connector)

	if s.ConnMaxLifetimeSecs > 0 {
		db.SetConnMaxLifetime(time.Duration(s.ConnMaxLifetimeSecs) * time.Second)
	}

	if s.MaxIdleConns > 0 {
		db.SetMaxIdleConns(s.MaxIdleConns)
	}

	if s.MaxOpenConns > 0 {
		db.SetMaxOpenConns(s.MaxOpenConns)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, err
}

func OpenServer(settings map[string]any) (*DuckDBServer, error) {
	srv := util.ConfigToStruct[DuckDBServer](settings)
	db, err := openDB(srv)
	if err != nil {
		return nil, err
	}
	srv.db = db
	return srv, nil
}
