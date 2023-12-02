package queuestorage

import (
	"scratchdata/pkg/queue"
	"scratchdata/pkg/storage"
)

type QueueStorage struct {
	queue   queue.QueueBackend
	storage storage.StorageBackend
}

func NewQueueStorageTransport(queue queue.QueueBackend, storage storage.StorageBackend) *QueueStorage {
	rc := &QueueStorage{
		queue:   queue,
		storage: storage,
	}

	return rc
}
