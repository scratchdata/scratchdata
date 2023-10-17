package ch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type (
	ClickhouseProvider interface {
		// FetchCredential retrieves clickhouse credential
		FetchCredential(ctx context.Context, serverKey string) (ClickhouseCred, error)

		// FetchConfig retrieve clickhouse configuration
		FetchConfig(ctx context.Context, serverKey string) (ClickhouseConfig, error)
	}

	ClickhouseCred struct {
		Protocol string `mapstructure:"protocol"`
		Host     string `mapstructure:"host"`
		HTTPPort string `mapstructure:"http_port"`
		TCPPort  string `mapstructure:"tcp_port"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
	}

	ClickhouseConfig struct {
		StoragePolicy       string `mapstructure:"storage_policy"`
		MaxOpenConns        int    `mapstructure:"max_open_conns"`
		MaxIdleConns        int    `mapstructure:"max_idle_conns"`
		ConnMaxLifetimeSecs int    `mapstructure:"conn_max_lifetime"`
	}

	ClickhouseServer struct {
		Credential ClickhouseCred   `mapstructure:"credential"`
		Config     ClickhouseConfig `mapstructure:"config"`

		clickhouse.Conn
		mu sync.Mutex
	}

	ClickhouseServers struct {
		Servers map[string]ClickhouseServer `mapstructure:"servers"`
		mu      sync.Mutex
	}
)

func (c *ClickhouseServer) Initialize(ctx context.Context) error {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", c.Credential.Host, c.Credential.TCPPort)},
		Auth: clickhouse.Auth{
			// Database: "default",
			Username: c.Credential.Username,
			Password: c.Credential.Password,
		},
		Debug:           false,
		MaxOpenConns:    c.Config.MaxOpenConns,
		MaxIdleConns:    c.Config.MaxIdleConns,
		ConnMaxLifetime: time.Second * time.Duration(c.Config.ConnMaxLifetimeSecs),
	}

	var err error
	if c.Conn, err = clickhouse.Open(opts); err != nil {
		return err
	}

	if err := c.Ping(ctx); err != nil {
		var exception *clickhouse.Exception
		if errors.As(err, &exception) {
			log.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	return nil
}

func (c *ClickhouseServers) FetchCredential(ctx context.Context, serverKey string) (ClickhouseCred, error) {
	// TODO: Implement once credentials are stored in database
	return ClickhouseCred{}, errors.New("unimplemented")
}

func (c *ClickhouseServers) FetchConfig(ctx context.Context, serverKey string) (ClickhouseConfig, error) {
	// TODO: Implement once credentials are stored in database
	return ClickhouseConfig{}, errors.New("unimplemented")
}
