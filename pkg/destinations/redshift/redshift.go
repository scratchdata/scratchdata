package redshift

import (
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"

	_ "github.com/lib/pq"
)

type RedshiftServer struct {
	Host     string `mapstructure:"redshift_host" schema:"redshift_host" form:"label:Host,type:text"`
	Port     int    `mapstructure:"redshift_port" schema:"redshift_port" form:"label:Port,type:number,default:5439"`
	Database string `mapstructure:"redshift_database" schema:"redshift_database" form:"label:Database,type:text"`
	Username string `mapstructure:"redshift_user" schema:"redshift_user" form:"label:User,type:text"`
	Password string `mapstructure:"redshift_password" schema:"redshift_password" form:"label:Password,type:password"`
	Schema   string `mapstructure:"redshift_schema" schema:"redshift_schema" form:"label:Schema,type:text,default:public"`

	S3Region          string `mapstructure:"s3_region" schema:"s3_region" form:"label:S3 Region,type:text"`
	S3AccessKeyId     string `mapstructure:"s3_access_key_id" schema:"s3_access_key_id" form:"label:S3 Access Key ID,type:text"`
	S3SecretAccessKey string `mapstructure:"s3_secret_access_key" schema:"s3_secret_access_key" form:"label:S3 Secret Access Key,type:password"`
	S3Bucket          string `mapstructure:"s3_bucket" schema:"s3_bucket" form:"label:S3 Bucket,type:text"`
	S3FilePrefix      string `mapstructure:"s3_file_prefix" schema:"s3_file_prefix" form:"label:S3 File Prefix,type:text"`

	DeleteFromS3 bool `mapstructure:"delete_from_s3" schema:"delete_from_s3" form:"label:Delete From S3,type:bool"`
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
