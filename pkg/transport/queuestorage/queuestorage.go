package queuestorage

import (
	"errors"
	"os"
	"sync"
	"time"

	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"

	"github.com/rs/zerolog/log"
)

var DefaultWriterOptions = WriterOptions{
	DataDir:     "./data",
	MaxFileSize: 100 * 1024 * 1024, // 100MB
	MaxRows:     1_000,
	MaxFileAge:  1 * time.Hour,
}

type WriterOptions struct {
	DataDir     string
	MaxFileSize int64
	MaxRows     int64
	MaxFileAge  time.Duration
}

type QueueStorageParam struct {
	Queue   queue.QueueBackend
	Storage filestore.StorageBackend

	WriterOpt    WriterOptions // TODO: Refactor use of this
	TimeProvider func() time.Time
}

type QueueStorage struct {
	queue   queue.QueueBackend
	storage filestore.StorageBackend

	DataDir string
	Workers int

	fws          map[string]*FileWriter
	fwsMu        sync.Mutex
	closedFiles  chan FileWriterInfo
	timeProvider func() time.Time

	wg   sync.WaitGroup
	done chan bool

	opt WriterOptions
}

func NewQueueStorageTransport(param QueueStorageParam) *QueueStorage {
	rc := &QueueStorage{
		queue:        param.Queue,
		storage:      param.Storage,
		timeProvider: param.TimeProvider,
		opt:          param.WriterOpt,

		fws:         make(map[string]*FileWriter),
		closedFiles: make(chan FileWriterInfo),
	}

	return rc
}

func (s *QueueStorage) StartProducer() error {
	log.Info().Msg("Starting data producer")
	return nil
}

func (s *QueueStorage) StopProducer() error {
	log.Info().Msg("Stopping data producer")

	var err error
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	for k, v := range s.fws {
		if closeErr := v.Close(); closeErr != nil {
			log.Error().Err(closeErr).Msg("unable to close file")
			err = errors.Join(err, closeErr)
		}
		delete(s.fws, k)
	}

	return err
}

func (s *QueueStorage) Write(databaseConnectionId string, data []byte) (err error) {
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	fw, ok := s.fws[databaseConnectionId]
	if !ok {
		var err error
		fw, err = NewFileWriter(FileWriterParam{
			Key:         databaseConnectionId,
			Dir:         s.opt.DataDir,
			MaxFileSize: s.opt.MaxFileSize,
			MaxRows:     s.opt.MaxRows,
			MaxFileAge:  s.opt.MaxFileAge,

			Queue:   s.queue,
			Storage: s.storage,
		})
		if err != nil {
			return err
		}
		s.fws[databaseConnectionId] = fw
	}

	if _, err = fw.Write(data); err != nil {
		return err
	}

	return nil
}

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
