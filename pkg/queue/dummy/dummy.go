package dummy

import (
	"errors"
)

// Queue implements an in-memory queue.QueueBackend
type DummyQueue struct {
}

// Enqueue implements queue.QueueBackend.Enqueue
func (q *DummyQueue) Enqueue(message []byte) error {
	return errors.New("Enqueue not implemented for dummy queue")
}

// Dequeue implememnts queue.QueueBackend.Dequeue
func (q *DummyQueue) Dequeue() ([]byte, error) {
	return []byte{}, errors.New("Dequeue not implemented for dummy queue")
}
