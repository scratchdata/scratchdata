package queue

import (
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/queue/memory"
	"github.com/scratchdata/scratchdata/pkg/storage/queue/sqs"
)

type Queue interface {
	Enqueue(value []byte) error
	Dequeue() ([]byte, bool)
}

func NewQueue(conf config.Queue) (Queue, error) {
	switch conf.Type {
	case "memory":
		return memory.NewQueue(conf.Settings)
	case "sqs":
		return sqs.NewQueue(conf.Settings)
	}

	return nil, nil
}
