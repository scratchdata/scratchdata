package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
)

type ClickhouseServer struct {
	HTTPProtocol string `mapstructure:"protocol"`
	Host         string `mapstructure:"host"`
	HTTPPort     int    `mapstructure:"http_port"`
	TCPPort      int    `mapstructure:"tcp_port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`

	StoragePolicy string `mapstructure:"storage_policy"`

	MaxOpenConns        int  `mapstructure:"max_open_conns"`
	MaxIdleConns        int  `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int  `mapstructure:"conn_max_lifetime_secs"`
	TLS                 bool `mapstructure:"tls"`
}

func (s *ClickhouseServer) createConn() (driver.Conn, error) {

	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", s.Host, s.TCPPort)},
		Auth: clickhouse.Auth{
			Username: s.Username,
			Password: s.Password,
		},
		Debug:           false,
		MaxOpenConns:    s.MaxOpenConns,
		MaxIdleConns:    s.MaxIdleConns,
		ConnMaxLifetime: time.Second * time.Duration(s.ConnMaxLifetimeSecs),
		DialTimeout:     120 * time.Second,
	}

	if s.TLS {
		options.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	var ctx = context.Background()
	var conn, err = clickhouse.Open(options)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Err(err).
				Int("code", int(exception.Code)).
				Str("message", exception.Message).
				Str("stackTrace", exception.StackTrace).
				Send()
		}
		return nil, err
	}

	return conn, nil
}
