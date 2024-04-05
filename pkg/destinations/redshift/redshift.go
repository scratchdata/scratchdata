package redshift

import (
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"

	_ "github.com/lib/pq"
)

type RedshiftServer struct {
	Host     string `mapstructure:"redshift_host" form_type:"text" form_label:"Host"`
	Port     int    `mapstructure:"redshift_port" form_type:"number" form_label:"Port"`
	Username string `mapstructure:"redshift_user" form_type:"number" form_label:"User"`
	Password string `mapstructure:"redshift_password" form_type:"password" form_label:"Password"`
	Database string `mapstructure:"redshift_database" form_type:"password" form_label:"Database"`
	Schema   string `mapstructure:"redshift_schema" form_type:"password" form_label:"Schema"`

	S3Region          string `mapstructure:"s3_region" form_type:"text" form_label:"S3 Region"`
	S3AccessKeyId     string `mapstructure:"s3_access_key_id" form_type:"text" form_label:"S3 Access Key ID"`
	S3SecretAccessKey string `mapstructure:"s3_secret_access_key" form_type:"password" form_label:"S3 Secret Access Key"`
	S3Bucket          string `mapstructure:"s3_bucket" form_type:"text" form_label:"S3 Bucket"`
	S3FilePrefix      string `mapstructure:"s3_file_prefix" form_type:"text" form_label:"S3 File Prefix"`

	DeleteFromS3 bool `mapstructure:"delete_from_s3" form_type:"bool" form_label:"Delete From S3"`
	conn         *sql.DB
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
		log.Error().Err(err).Msg("redshift conn error")
		return nil, err
	}
	log.Printf("Connecting to Redshift %v", url)
	if err = db.Ping(); err != nil {
		log.Error().Err(err).Msg("redshift ping error")
		return nil, err
	}
	log.Info().Msg("Connected to Redshift")
	return db, nil
}

func OpenServer(settings map[string]any) (*RedshiftServer, error) {

	srv := util.ConfigToStruct[RedshiftServer](settings)
	if srv.Schema == "" {
		srv.Schema = "public"
	}

	conn, err := openConn(srv)
	if err != nil {
		log.Error().Err(err).Msg("Redshift OpenServer Error")
		return nil, err
	}
	srv.conn = conn
	return srv, nil
}

func (s *RedshiftServer) Close() error {
	return s.conn.Close()
}
