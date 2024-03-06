package memory

import (
	"bytes"
	"errors"
	"testing"

	"github.com/scratchdata/scratchdata/pkg/queue"
)

func TestQueue(t *testing.T) {
	messages := [][]byte{
		[]byte(`hello`),
		[]byte(`world`),
	}
	mq := NewQueue()

	if _, err := mq.Dequeue(); !errors.Is(err, queue.ErrEmpyQueue) {
		t.Fatalf("Expected error %s; Got %v", queue.ErrEmpyQueue, err)
	}

	for _, msg := range messages {
		if err := mq.Enqueue(msg); err != nil {
			t.Fatalf("Enqueue(%s): %s", msg, err)
		}
	}

	for i, msg := range messages {
		res, err := mq.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue(%d): %s", i, err)
		}
		if !bytes.Equal(msg, res) {
			t.Fatalf("Dequeue(%d): Expected '%s'; Got '%s'", i, msg, res)
		}
	}

	if _, err := mq.Dequeue(); !errors.Is(err, queue.ErrEmpyQueue) {
		t.Fatalf("Expected error %s; Got %v", queue.ErrEmpyQueue, err)
	}
}
