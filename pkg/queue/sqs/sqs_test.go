package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/queue"

	"github.com/ory/dockertest/v3"
	"github.com/rs/zerolog/log"
)

func waitForLocalstack(pool *dockertest.Pool, endpoint string) error {
	return pool.Retry(func() error {
		resp, err := http.Get(endpoint + "/_localstack/init/ready")
		if err != nil {
			log.Info().Err(err).Msg("waitForLocalstack: connect")
			return err
		}
		defer resp.Body.Close()
		status := struct {
			Completed bool
			Scripts   []struct {
				Name  string
				State string
				Stage string
			}
		}{}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			log.Info().Err(err).Msg("waitForLocalstack: decode")
			return err
		}
		if status.Completed {
			log.Info().Any("status", status).Msg("waitForLocalstack: compelted")
			return nil
		}
		return fmt.Errorf("not ready")
	})
}

func TestQueue(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Cannot get working directory: %s", err)
	}

	queueName := "test-q"
	conf := config.SQS{
		AccessKeyId:     "localstack",
		SecretAccessKey: "localstack",
		Region:          "us-east-1",
		SqsURL:          queueName,
	}
	dockerOpts := &dockertest.RunOptions{
		Repository: "localstack/localstack",
		Env: []string{
			"AWS_ACCESS_KEY_ID=" + conf.AccessKeyId,
			"AWS_SECRET_ACCESS_KEY=" + conf.SecretAccessKey,
			"AWS_DEFAULT_REGION=" + conf.Region,
			"TEST_QUEUE_NAME=" + queueName,
		},
		Mounts: []string{
			wd + "/testdata/init-sqs.sh:/etc/localstack/init/ready.d/init-sqs.sh",
		},
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("Create pool: %s", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Fatalf("Ping Docker: %s", err)
	}
	resource, err := pool.RunWithOptions(dockerOpts)
	if err != nil {
		t.Fatalf("Run container: %s", err)
	}
	t.Cleanup(func() {
		if err := pool.Purge(resource); err != nil {
			t.Logf("Purge resource: %s", err)
		}
	})

	addr := resource.GetHostPort("4566/tcp")
	if addr == "" {
		t.Fatalf("Cannot get queue endpoint")
	}
	conf.Endpoint = "http://" + addr

	if err := waitForLocalstack(pool, conf.Endpoint); err != nil {
		t.Fatalf("localstack didn't start: %s", err)
	}

	q := NewQueue(conf)

	// we haven't enqueued anything, so the queue should now be empty
	if _, err := q.Dequeue(); !errors.Is(err, queue.ErrEmpyQueue) {
		t.Fatalf("Expected error '%s'; Got '%v'", queue.ErrEmpyQueue, err)
	}

	messages := map[string]bool{
		"hello": true,
		"world": true,
	}

	for msg := range messages {
		if err := q.Enqueue([]byte(msg)); err != nil {
			t.Fatalf("Enqueue: %s: %s", msg, err)
		}
	}

	for range messages {
		msg, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue: %s", err)
		}
		if !messages[string(msg)] {
			t.Fatalf("Dequeue: unknown message: %s", msg)
		}
		// remove it from the list, to detect failure to remove it from the queue
		delete(messages, string(msg))
	}

	if len(messages) != 0 {
		t.Fatalf("Failed to dequeue message(s): %v", messages)
	}

	// we've dequeued everything, so the queue should now be empty
	if _, err := q.Dequeue(); !errors.Is(err, queue.ErrEmpyQueue) {
		t.Fatalf("Expected error '%s'; Got '%v'", queue.ErrEmpyQueue, err)
	}
}
