package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	queue_models "github.com/scratchdata/scratchdata/pkg/storage/queue/models"
)

type ScratchDataWorker struct {
	Config             config.Workers
	StorageServices    *storage.Services
	destinationManager *destinations.DestinationManager
}

func (w *ScratchDataWorker) Produce(ctx context.Context, ch chan<- *models.Message, wg *sync.WaitGroup, messageType models.MessageType) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			// close(ch)
			// close producer channel
			return
		default:
			item, ok := w.StorageServices.Database.Dequeue(messageType, "worker-label")
			if !ok {
				time.Sleep(1 * time.Second)
			}
			ch <- item
		}
	}
}

func (w *ScratchDataWorker) Consume(ctx context.Context, ch <-chan *models.Message, threadId int, wg *sync.WaitGroup) {
	log.Debug().Int("thread", threadId).Msg("Starting worker")
	defer wg.Done()

	// hostname, _ := os.Hostname()
	// workerLabel := fmt.Sprintf("%s-%d", hostname, threadId)

	// queuesToPoll := []models.MessageType{models.InsertData, models.CopyData}
	// var queueIdxToPoll uint8

	for item := range ch {
		// queueToPoll := queuesToPoll[queueIdxToPoll]
		// item, ok := w.StorageServices.Database.Dequeue(queueToPoll, workerLabel)

		message, err := w.messageToStruct([]byte(item.Message))
		if err != nil {
			log.Error().Err(err).Int("thread", threadId).Str("message", item.Message).Msg("Unable to decode message")
			continue
		}

		switch item.MessageType {
		case models.InsertData:
			err = w.processMessage(threadId, message)
		default:
			log.Error().Err(err).Int("thread", threadId).Interface("message", item).Msg("Unrecognized message type")
			continue
		}

		if err == nil {
			deleteErr := w.StorageServices.Database.Delete(item.ID)
			if deleteErr != nil {
				log.Error().Err(deleteErr).Uint("message_id", item.ID).Msg("Unable to delete message from queue")
			}
		} else {
			log.Error().Err(err).Int("thread", threadId).Interface("message", message).Msg("Unable to process message")
		}

		// select {
		// case <-ctx.Done():
		// log.Debug().Int("thread", threadId).Msg("Stopping worker")
		// return
		// default:
		// }
	}
}

// func (w *ScratchDataWorker) Start(ctx context.Context, threadId int) {
// 	log.Debug().Int("thread", threadId).Msg("Starting worker")

// 	hostname, _ := os.Hostname()
// 	workerLabel := fmt.Sprintf("%s-%d", hostname, threadId)

// 	queuesToPoll := []models.MessageType{models.InsertData, models.CopyData}
// 	var queueIdxToPoll uint8

// 	for {
// 		queueToPoll := queuesToPoll[queueIdxToPoll]
// 		item, ok := w.StorageServices.Database.Dequeue(queueToPoll, workerLabel)

// 		if !ok {
// 			time.Sleep(1 * time.Second)
// 		} else {
// 			message, err := w.messageToStruct([]byte(item.Message))
// 			if err != nil {
// 				log.Error().Err(err).Int("thread", threadId).Str("message", item.Message).Msg("Unable to decode message")
// 				continue
// 			}

// 			switch item.MessageType {
// 			case models.InsertData:
// 				err = w.processMessage(threadId, message)
// 			default:
// 				log.Error().Err(err).Int("thread", threadId).Any("message", item).Msg("Unrecognized msg type")
// 				continue
// 			}

// 			if err == nil {
// 				deleteErr := w.StorageServices.Database.Delete(item.ID)
// 				if deleteErr != nil {
// 					log.Error().Err(deleteErr).Uint("message_id", item.ID).Msg("Unable to delete message from queue")
// 				}
// 			} else {
// 				log.Error().Err(err).Int("thread", threadId).Interface("message", message).Msg("Unable to process message")
// 			}
// 		}

// 		select {
// 		case <-ctx.Done():
// 			log.Debug().Int("thread", threadId).Msg("Stopping worker")
// 			return
// 		default:
// 		}
// 	}
// }

func (w *ScratchDataWorker) processMessage(threadId int, message queue_models.FileUploadMessage) error {
	destination, err := w.destinationManager.Destination(context.TODO(), message.DatabaseID)
	if err != nil {
		return err
	}

	fileIdent := filepath.Base(message.Key)
	fileName := fmt.Sprintf("%d_%s_%s.ndjson", message.DatabaseID, message.Table, fileIdent)
	filePath := filepath.Join(w.Config.DataDirectory, fileName)

	err = w.downloadFile(filePath, message.Key)
	if err != nil {
		return err
	}

	err = destination.CreateEmptyTable(message.Table)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)

	if err != nil {
		return err
	}

	err = destination.CreateColumns(message.Table, filePath)
	if err != nil {
		return err
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}

	err = destination.InsertFromNDJsonFile(message.Table, filePath)
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		log.Error().Err(err).Int("thread", threadId).Str("filename", filePath).Msg("Unable to close temp file")
	}

	err = os.Remove(filePath)
	if err != nil {
		log.Error().Err(err).Int("thread", threadId).Str("filename", filePath).Msg("Unable to remove temp file")
	}

	return nil
}

func (w *ScratchDataWorker) messageToStruct(item []byte) (queue_models.FileUploadMessage, error) {
	message := queue_models.FileUploadMessage{}
	err := json.Unmarshal(item, &message)
	return message, err
}

func (w *ScratchDataWorker) downloadFile(path string, key string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	err = w.StorageServices.BlobStore.Download(key, file)
	if err != nil {
		return err
	}

	return file.Close()
}

func RunWorkers(ctx context.Context, config config.Workers, storageServices *storage.Services, destinationManager *destinations.DestinationManager) {
	err := os.MkdirAll(config.DataDirectory, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Str("directory", config.DataDirectory).Msg("Unable to create folder for workers")
		return
	}

	workers := &ScratchDataWorker{
		Config:             config,
		StorageServices:    storageServices,
		destinationManager: destinationManager,
	}

	var values chan *models.Message

	log.Debug().Msg("Starting Producers")
	var producerWg sync.WaitGroup

	producerWg.Add(2)
	go workers.Produce(ctx, values, &producerWg, models.InsertData)
	go workers.Produce(ctx, values, &producerWg, models.CopyData)

	log.Debug().Msg("Starting Consumers")
	var consumerWg sync.WaitGroup
	for i := 0; i < config.Count; i++ {
		consumerWg.Add(1)
		go workers.Consume(ctx, values, i, &consumerWg)
	}

	producerWg.Wait()

	log.Debug().Msg("Closing Producer")
	close(values)

	log.Debug().Msg("Closing Consumers...")
	consumerWg.Wait()
}
