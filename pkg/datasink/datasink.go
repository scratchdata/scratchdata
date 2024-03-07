package datasink

import (
	"errors"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/datasink/memory"
)

type DataSink interface {
	WriteData(databaseID int64, table string, data []byte) error
}

func NewDataSink(conf config.DataSink, storage *models.StorageServices) (DataSink, error) {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryDataSink(conf, storage)
	}

	return nil, errors.New("Unsupported data sink")
}
