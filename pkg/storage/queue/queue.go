package queue

import (
	"errors"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/queue/memory"
)

type Queue interface {
	Enqueue(value []byte) error
	Dequeue() ([]byte, error)
}

func NewQueue(conf config.Queue) (Queue, error) {
	switch conf.Type {
	case "memory":
		return memory.NewQueue(conf)
	}

	return nil, errors.New("Unsupported queue type")
}
