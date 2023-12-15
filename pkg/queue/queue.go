package queue

import "errors"

var (
	ErrEmpyQueue = errors.New("empty queue")
)

// QueueBackend is the interface implemented by queue backends
type QueueBackend interface {
	// Enqueue pushes message into the queue
	Enqueue(message []byte) error

	// Dequeue pops a message from the queue or returns an error
	//
	// If the queue is empty, (wrapped) error ErrEmptyQueue is returned
	Dequeue() ([]byte, error)
}
