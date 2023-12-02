package queue

type QueueBackend interface {
	Enqueue(message []byte) error
	Dequeue() ([]byte, error)
}
