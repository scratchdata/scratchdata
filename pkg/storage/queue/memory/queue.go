package memory

import (
	"sync"
)

type Queue struct {
	mu    sync.Mutex
	items [][]byte
}

func (q *Queue) Enqueue(message []byte) error {
	// copy message to avoid external modification
	message = append([]byte(nil), message...)

	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, message)
	return nil
}

func (q *Queue) Dequeue() ([]byte, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil, false
	}

	message := q.items[0]
	q.items = q.items[1:]
	return message, true
}

// NewQueue returns a new initialized Queue
func NewQueue(conf map[string]any) (*Queue, error) {
	return &Queue{}, nil
}
