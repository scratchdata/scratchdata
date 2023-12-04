package queuestorage

import (
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"

	"github.com/rs/zerolog/log"
)

type QueueStorage struct {
	queue   queue.QueueBackend
	storage filestore.StorageBackend
}

func NewQueueStorageTransport(queue queue.QueueBackend, storage filestore.StorageBackend) *QueueStorage {
	rc := &QueueStorage{
		queue:   queue,
		storage: storage,
	}

	return rc
}

// func (s QueueStorage) GetAccountManager() accounts.AccountManager { return nil }

func (s QueueStorage) StartProducer() error {
	log.Info().Msg("Starting data producer")
	return nil
}

func (s QueueStorage) StopProducer() error {
	log.Info().Msg("Stopping data producer")
	return nil
}

func (s QueueStorage) Write(databaseConnectionId string, data []byte) error { return nil }

func (s QueueStorage) StartConsumer() error { return nil }
func (s QueueStorage) StopConsumer() error  { return nil }
