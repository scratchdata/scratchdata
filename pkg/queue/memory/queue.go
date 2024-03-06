package memory

import (
	"fmt"
	"sync"

	"github.com/scratchdata/scratchdata/pkg/queue"
)

var (
	_ queue.QueueBackend = (*Queue)(nil)
)

// Queue implements an in-memory queue.QueueBackend
type Queue struct {
	mu sync.Mutex

	ents [][]byte
}

// Enqueue implements queue.QueueBackend.Enqueue
func (q *Queue) Enqueue(message []byte) error {
	// copy message to avoid external modification
	message = append([]byte(nil), message...)

	q.mu.Lock()
	defer q.mu.Unlock()

	q.ents = append(q.ents, message)
	return nil
}

// Dequeue implememnts queue.QueueBackend.Dequeue
func (q *Queue) Dequeue() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.ents) == 0 {
		return nil, fmt.Errorf("Queue.Dequeue: %w", queue.ErrEmpyQueue)
	}
	message := q.ents[0]
	q.ents = q.ents[1:]
	return message, nil
}

// NewQueue returns a new initialized Queue
func NewQueue() *Queue {
	return &Queue{}
}
