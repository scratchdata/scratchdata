package queuestorage

import (
	"os"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
	"sync"

	"github.com/rs/zerolog/log"
)

type QueueStorage struct {
	queue   queue.QueueBackend
	storage filestore.StorageBackend

	DataDir string
	Workers int

	wg   sync.WaitGroup
	done chan bool
}

func NewQueueStorageTransport(queue queue.QueueBackend, storage filestore.StorageBackend) *QueueStorage {
	rc := &QueueStorage{
		queue:   queue,
		storage: storage,
	}

	return rc
}

func (s *QueueStorage) StartProducer() error {
	log.Info().Msg("Starting data producer")
	return nil
}

func (s *QueueStorage) StopProducer() error {
	log.Info().Msg("Stopping data producer")
	return nil
}

func (s *QueueStorage) Write(databaseConnectionId string, data []byte) error { return nil }

func (s *QueueStorage) StartConsumer() error {
	log.Info().Msg("Starting DB importer")

	err := os.MkdirAll(s.DataDir, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Msg("unable to make required directories")
	}

	s.wg.Add(1)
	// go s.produceMessages()

	s.wg.Add(s.Workers)
	for i := 0; i < s.Workers; i++ {
		// go s.consumeMessages(i)
	}

	return nil
}

func (s *QueueStorage) StopConsumer() error {
	log.Info().Msg("Shutting down data importer")
	s.done <- true
	s.wg.Wait()
	return nil
}
