package redshift

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/scratchdata/scratchdata/util"
)

type RedshiftServer struct {
	DeleteFromS3      bool   `mapstructure:"delete_from_s3"`
	RedshiftHost      string `mapstructure:"redshift_host"`
	RedshiftPort      int    `mapstructure:"redshift_port"`
	RedshiftUsername  string `mapstructure:"redshift_user"`
	RedshiftPassowrd  string `mapstructure:"redshift_password"`
	RedshiftDBName    string `mapstructure:"redshift_dbname"`
	S3Region          string `mapstructure:"s3_region"`
	S3AccesKeyId      string `mapstructure:"s3_access_key_id"`
	S3SecretAccessKey string `mapstructure:"s3_secret_access_key"`
	S3Bucket          string `mapstructure:"s3_bucket"`
	conn              *sql.DB
}

func openConn(r *RedshiftServer) (*sql.DB, error) {
	connectionString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s", r.RedshiftHost, r.RedshiftPort, r.RedshiftUsername, r.RedshiftPassowrd, r.RedshiftDBName)
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
