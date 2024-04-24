package postgres

import (
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"

	_ "github.com/lib/pq"
)

type PostgresServer struct {
	Host     string `mapstructure:"host" schema:"host" form:"label:Host,type:text"`
	Port     int    `mapstructure:"port" schema:"port" form:"label:Port,type:number,default:5432"`
	Database string `mapstructure:"database" schema:"database" form:"label:Database,type:text"`
	Username string `mapstructure:"user" schema:"user" form:"label:User,type:text"`
	Password string `mapstructure:"password" schema:"password" form:"label:Password,type:password"`
	Schema   string `mapstructure:"schema" schema:"schema" form:"label:Schema,type:text,default:public"`

	conn *sql.DB
}

func openConn(s *PostgresServer) (*sql.DB, error) {
	url := fmt.Sprintf("user=%v password=%v host=%v port=%v dbname=%v connect_timeout=5 sslmode=require",
		s.Username,
		s.Password,
		s.Host,
		s.Port,
		s.Database,
	)

	var err error
	var db *sql.DB

	if db, err = sql.Open("postgres", url); err != nil {
		log.Error().Err(err).Msg("postgres conn error")
		return nil, err
	}
	log.Printf("Connecting to Postgres %v", url)
	if err = db.Ping(); err != nil {
		log.Error().Err(err).Msg("postgres ping error")
		return nil, err
	}
	log.Info().Msg("Connected to Postgres")
	return db, nil
}

func OpenServer(settings map[string]any) (*PostgresServer, error) {

	srv := util.ConfigToStruct[PostgresServer](settings)
	if srv.Schema == "" {
		srv.Schema = "public"
	}

	conn, err := openConn(srv)
	if err != nil {
		log.Error().Err(err).Msg("Postgres OpenServer Error")
		return nil, err
	}
	srv.conn = conn
	return srv, nil
}

func (s *PostgresServer) Close() error {
	return s.conn.Close()
}
