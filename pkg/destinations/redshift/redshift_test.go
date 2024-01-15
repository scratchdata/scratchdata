package redshift

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

func TestRedshift(t *testing.T) {
	database := "testdb"
	username := "testuser"
	password := "testpass"

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Create pool: %s", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Fatalf("Ping Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(
		&dockertest.RunOptions{
			Repository: "hearthsim/pgredshift",
			Env: []string{
				"POSTGRES_DB=" + database,
				"POSTGRES_USER=" + username,
				"POSTGRES_PASSWORD=" + password,
			},
		},
		func(config *docker.HostConfig) {
			// set AutoRemove to true so that stopped container goes away by itself
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		},
	)
	if err != nil {
		t.Fatalf("Run container: %s", err)
	}

	addr := resource.GetHostPort("5432/tcp")
	if addr == "" {
		t.Fatalf("Cannot server addr")
	}

	// postgres can sometimes take a while to start, unfortunately
	pool.MaxWait = 2 * time.Minute
	resource.Expire(3 * 60)

	dbSettings := map[string]any{
		"tls":                  false,
		"endpoint":             addr + "/" + database,
		"username":             username,
		"password":             password,
		"database_is_postgres": true,
	}
	var srv *RedshiftServer
	err = pool.Retry(func() error {
		srv, err = OpenServer(dbSettings)
		return err
	})
	if err != nil {
		t.Fatalf("Cannot open server: %s", err)
	}

	if err := srv.InsertBatchFromNDJson("tbl", strings.NewReader(`{"msg":"hello world"}`)); err != nil {
		t.Fatalf("Cannot insert JSON: %s", err)
	}
	buf := &bytes.Buffer{}
	if err := srv.QueryJSON(`select * from tbl`, buf); err != nil {
		t.Fatalf("Cannot query JSON: %s", err)
	}
	type Msg struct{ Msg string }
	vals := []Msg{}
	if err := json.Unmarshal(buf.Bytes(), &vals); err != nil {
		t.Fatalf("Cannot decode JSON: %s", err)
	}
	exp := Msg{Msg: "hello world"}
	if len(vals) != 1 || vals[0] != exp {
		t.Fatalf(`Expected [%+v]; Got %+v`, exp, vals)
	}
}
