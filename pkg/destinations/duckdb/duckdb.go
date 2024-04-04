package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"

	"github.com/marcboeker/go-duckdb"
)

type DuckDBServer struct {
	Database string `mapstructure:"database" form_type:"text" form_label:"Database Name"`
	Token    string `mapstructure:"token" form_type:"password" form_label:"MotherDuck Token"`

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
	var connectionString string

	if s.InMemory {
		connectionString = ""
	} else if s.File != "" {
		directory := filepath.Dir(s.File)
		err := os.MkdirAll(directory, os.ModePerm)
		if err != nil {
			return nil, err
		}
		connectionString = s.File
	} else if s.Database != "" && s.Token != "" {
		if strings.Contains(strings.ToLower(s.Database), "saas_mode") {
			return nil, errors.New("db cannot be named saas_mode")
		}
		if strings.Contains(strings.ToLower(s.Token), "saas_mode") {
			return nil, errors.New("token cannot be named saas_mode")
		}

		connectionString = "md:" + s.Database + "?motherduck_saas_mode=true&motherduck_token=" + s.Token

		// connectionString = "md:" + s.Database + "?motherduck_token=" + s.Token
	} else {
		return nil, errors.New("Must specify DuckDB connection type: in memory, file, or MotherDuck credentials")
	}

	homedir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	log.Trace().Str("homedir", homedir).Send()

	connector, err := duckdb.NewConnector(connectionString, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			fmt.Sprintf("SET home_directory = '%s'", homedir),
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
