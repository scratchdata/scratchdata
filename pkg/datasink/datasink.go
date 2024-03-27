package datasink

import (
	"context"
	"errors"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/datasink/filesystem"
	"github.com/scratchdata/scratchdata/pkg/datasink/memory"
	"github.com/scratchdata/scratchdata/pkg/storage"
)

type DataSink interface {
	Start(context.Context) error
	WriteData(databaseID int64, table string, data []byte) error
}

func NewDataSink(conf config.DataSink, storage *storage.Services) (DataSink, error) {
	switch conf.Type {
	case "memory":
		return memory.NewMemoryDataSink(storage)
	case "filesystem":
		return filesystem.NewFilesystemDataSink(conf.Settings, storage)
	}

	return nil, errors.New("Unsupported data sink")
}
