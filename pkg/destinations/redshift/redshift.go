package redshift

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/scratchdata/scratchdata/util"
)

type RedshiftServer struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	conn     *sql.DB
}

func openConn(r *RedshiftServer) (*sql.DB, error) {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s", r.Host, r.Port, r.Username, r.Password, r.Database)
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func OpenServer(settings map[string]any) (*RedshiftServer, error) {
	srv := util.ConfigToStruct[RedshiftServer](settings)
	conn, err := openConn(srv)
	if err != nil {
		return nil, fmt.Errorf("Redshift OpenServer Error: %w", err)
	}
	srv.conn = conn
	return srv, nil
}

func (s *RedshiftServer) Close() error {
	return s.conn.Close()
}
