package memory

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/models"
	queue_models "github.com/scratchdata/scratchdata/pkg/storage/queue/models"
	"github.com/scratchdata/scratchdata/util"
)

type DataSink struct {
	storage *models.StorageServices
	snow    *snowflake.Node
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

func NewMemoryDataSink(conf config.DataSink, storage *models.StorageServices) (*DataSink, error) {
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
