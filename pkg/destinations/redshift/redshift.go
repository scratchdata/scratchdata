package redshift

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/scratchdata/scratchdata/util"
)

type RedshiftServer struct {
	Host     string `mapstructure:"redshift_host"`
	Port     int    `mapstructure:"redshift_port"`
	Username string `mapstructure:"redshift_user"`
	Password string `mapstructure:"redshift_password"`
	Database string `mapstructure:"redshift_database"`
	Schema   string `mapstructure:"redshift_schema"`

	S3Region          string `mapstructure:"s3_region"`
	S3AccessKeyId     string `mapstructure:"s3_access_key_id"`
	S3SecretAccessKey string `mapstructure:"s3_secret_access_key"`
	S3Bucket          string `mapstructure:"s3_bucket"`

	conn *sql.DB
}

func openConn(s *RedshiftServer) (*sql.DB, error) {
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v",
		s.Username,
		s.Password,
		s.Host,
		s.Port,
		s.Database)

	var err error
	var db *sql.DB

	if db, err = sql.Open("postgres", url); err != nil {
		return nil, fmt.Errorf("redshift conn error : (%v)", err)
	}
	log.Printf("Connecting to Redshift %v", url)
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("redshift ping error : (%v)", err)
	}
	log.Println("Connected to Redshift")
	return db, nil
}

func OpenServer(settings map[string]any) (*RedshiftServer, error) {

	srv := util.ConfigToStruct[RedshiftServer](settings)
	if srv.Schema == "" {
		srv.Schema = "public"
	}

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
