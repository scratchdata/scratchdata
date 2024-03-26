package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/scratchdata/scratchdata/pkg/storage"
	queue_models "github.com/scratchdata/scratchdata/pkg/storage/queue/models"
	"github.com/scratchdata/scratchdata/util"
)

type DataSink struct {
	storage *storage.Services
	snow    *snowflake.Node
}

func (m DataSink) Start(ctx context.Context) error {
	return nil
}

func (m DataSink) WriteData(databaseID int64, table string, data []byte) error {
	fileId := m.snow.Generate()
	key := fmt.Sprintf("%d/%s/%d.ndjson", databaseID, table, fileId.Int64())
	reader := bytes.NewReader(data)

	uploadErr := m.storage.BlobStore.Upload(key, reader)
	if uploadErr != nil {
		return uploadErr
	}

	uploadMessage := queue_models.FileUploadMessage{
		DatabaseID: databaseID,
		Table:      table,
		Key:        key,
	}

	// TODO: log payload for replay
	message, err := json.Marshal(uploadMessage)
	if err != nil {
		return err
	}

	// TODO: log payload for replay
	err = m.storage.Queue.Enqueue(message)
	if err != nil {
		return err
	}

	return nil
}

func NewMemoryDataSink(storage *storage.Services) (*DataSink, error) {
	snow, err := util.NewSnowflakeGenerator()
	if err != nil {
		return nil, err
	}

	rc := &DataSink{
		storage: storage,
		snow:    snow,
	}
	return rc, nil
}
