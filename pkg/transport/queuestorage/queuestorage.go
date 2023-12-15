package queuestorage

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"scratchdata/models"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"

	"github.com/rs/zerolog/log"
	"github.com/tidwall/sjson"
)

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
}

type QueueStorageTransportParam struct {
	Queue        queue.QueueBackend
	Storage      filestore.StorageBackend
	TimeProvider func() time.Time
}

func NewQueueStorageTransport(param QueueStorageTransportParam) *QueueStorage {
	rc := &QueueStorage{
		queue:        param.Queue,
		storage:      param.Storage,
		timeProvider: param.TimeProvider,

		fws:         make(map[string]*FileWriter),
		closedFiles: make(chan FileWriterInfo),
	}

	return rc
}

func (s *QueueStorage) StartProducer() error {
	log.Info().Msg("Starting data producer")
	go s.handleFileEvent()
	return nil
}

func (s *QueueStorage) StopProducer() error {
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	log.Info().Msg("Stopping data producer")
	//When stop is called, we want to make sure that we stop performing any more writes (the Write() function should return an error)
	//We then want to make sure any remaining data is flushed to disk and then uploaded to S3 and the Queue before returning

	for k, v := range s.fws {
		err := v.Close()
		if err != nil {
			log.Error().Err(err).Msg("unable to close file")
		}
		delete(s.fws, k)
	}

	return nil
}

func (s *QueueStorage) Write(databaseConnectionId string, data []byte) (err error) {
	rowID := ulid.Make().String()
	batchFile := rowID + ".ndjson"

	if data, err = sjson.SetBytes(data, "__row_id", rowID); err != nil {
		log.Err(err).Msg("unable to set __row_id in JSON")
	}

	if data, err = sjson.SetBytes(data, "__batch_file", batchFile); err != nil {
		log.Err(err).Msg("unable to set __batch_file in JSON")
	}

	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	fw, ok := s.fws[databaseConnectionId]
	if !ok {
		err := s.createFileWriter(databaseConnectionId, batchFile)
		if err != nil {
			return err
		}
	}

	if _, err = fw.WriteLn(data); err != nil {
		return err
	}

	return nil
}

func (s *QueueStorage) createFileWriter(dbID, batchFile string) error {
	s.fwsMu.Lock()
	defer s.fwsMu.Unlock()
	fw, err := NewFileWriter(NewFileWriterParam{
		Key:         dbID,
		Path:        batchFile,
		Notify:      s.closedFiles,
		MaxFileSize: MaxFileSize,
		MaxRows:     MaxRows,
		Expiry:      s.timeProvider().Add(MaxFileAge),
	})
	if err != nil {
		return err
	}
	s.fws[dbID] = fw
	fw = s.fws[dbID]
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

func (s *QueueStorage) handleFileEvent() {
	for {
		ev, ok := <-s.closedFiles
		if !ok {
			break
		}

		//  lock the mutex
		//  get the file writer for this file
		//  delete the file writer from the map
		//  unlock the mutex
		s.fwsMu.Lock()
		fw := s.fws[ev.Key]
		delete(s.fws, ev.Key)
		s.fwsMu.Unlock()

		fd, err := os.Open(ev.Path)
		if err != nil {
			log.Error().Err(err).
				Str("filePath", ev.Path).
				Msg("unable to open file")
			continue
		}
		if err := s.storage.Upload(ev.Path, fd); err != nil {
			log.Error().Err(err).
				Str("filePath", ev.Path).
				Msg("unable to upload file")
			continue
		}
		err = fd.Close()
		if err != nil {
			log.Error().Err(err).
				Str("filePath", ev.Path).
				Msg("unable to close file")
			continue
		}

		bb, err := json.Marshal(models.FileUploadMessage{
			Key:  ev.Key,
			Path: ev.Path,
		})
		if err := s.queue.Enqueue(bb); err != nil {
			log.Error().Err(err).
				Str("filePath", ev.Path).
				Msg("unable to enqueue file")
			continue
		}

		// TODO: Remove file after upload
		if err := fw.Close(); err != nil {
			log.Error().Err(err).Str("filePath", ev.Path).
				Msg("unable to close file")
			continue
		}
	}
	return
}
