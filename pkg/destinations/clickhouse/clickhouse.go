package clickhouse

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/scratchdata/scratchdata/pkg/util"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
)

type ClickhouseServer struct {
	Host         string `mapstructure:"host"`
	HTTPProtocol string `mapstructure:"http_protocol"`
	HTTPPort     int    `mapstructure:"http_port"`
	TCPPort      int    `mapstructure:"tcp_port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`
	TLS          bool   `mapstructure:"tls"`

	StoragePolicy string `mapstructure:"storage_policy"`

	MaxOpenConns        int `mapstructure:"max_open_conns"`
	MaxIdleConns        int `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int `mapstructure:"conn_max_lifetime_secs"`

	conn driver.Conn
}

func openConn(s *ClickhouseServer) (driver.Conn, error) {
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", s.Host, s.TCPPort)},
		Auth: clickhouse.Auth{
			Username: s.Username,
			Password: s.Password,
		},
		Debug:       false,
		DialTimeout: 120 * time.Second,
	}

	if s.MaxOpenConns > 0 {
		options.MaxOpenConns = s.MaxOpenConns
	}
	if s.MaxIdleConns > 0 {
		options.MaxIdleConns = s.MaxIdleConns
	}
	if s.ConnMaxLifetimeSecs > 0 {
		options.ConnMaxLifetime = time.Second * time.Duration(s.ConnMaxLifetimeSecs)
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

func (s *ClickhouseServer) Close() error {
	return s.conn.Close()
}

func (s *ClickhouseServer) httpQuery(query string) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s://%s:%d", s.HTTPProtocol, s.Host, s.HTTPPort)

	var jsonStr = []byte(query)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Clickhouse-User", s.Username)
	req.Header.Set("X-Clickhouse-Key", s.Password)
	req.Header.Set("X-Clickhouse-Database", s.Database)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("request failed")
		resp.Body.Close()
		return nil, err
	}

	if resp.StatusCode != 200 {
		errMsg, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			return nil, readErr
		}

		return nil, errors.New(string(errMsg))
	}

	return resp.Body, nil
}

func OpenServer(settings map[string]any) (*ClickhouseServer, error) {
	srv := util.ConfigToStruct[ClickhouseServer](settings)
	conn, err := openConn(srv)
	if err != nil {
		return nil, fmt.Errorf("OpenServer: %w", err)
	}
	srv.conn = conn
	return srv, nil
}
