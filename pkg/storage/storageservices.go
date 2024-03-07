package storage

import (
	"errors"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/destinations/clickhouse"
	"github.com/scratchdata/scratchdata/pkg/destinations/duckdb"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
)

type StorageServices interface {
	Database() database.Database
	Queue() QueueI
	Cache() CacheI
	BlobStore() BlobStoreI
	DataSink() DataSink
	Destination(databaseID int64) (destinations.Destination, error)
}

type QueueI interface{}
type CacheI interface{}
type BlobStoreI interface{}
type DataSink interface {
	WriteData(databaseID int64, table string, data []byte) error
}

type StorageService struct {
	database  database.Database
	cache     CacheI
	queue     QueueI
	blobStore BlobStoreI
	dataSink  DataSink
}

func (s *StorageService) Database() database.Database {
	return s.database
}

func (s *StorageService) Queue() QueueI {
	return s.queue
}

func (s *StorageService) Cache() CacheI {
	return s.cache
}

func (s *StorageService) BlobStore() BlobStoreI {
	return s.blobStore
}

func (s *StorageService) DataSink() DataSink {
	return s.dataSink
}

func (s *StorageService) Destination(databaseID int64) (destinations.Destination, error) {
	creds, err := s.database.GetDestinationCredentials(databaseID)
	if err != nil {
		return nil, err
	}

	switch creds.Type {
	case "duckdb":
		return duckdb.OpenServer(creds.Settings)
	case "clickhouse":
		return clickhouse.OpenServer(creds.Settings)
	}
	// TODO cache connection

	return nil, errors.New("Unrecognized database type: " + creds.Type)
}

func NewStorageService(database database.Database, cache CacheI, queue QueueI, blobStore BlobStoreI, dataSink DataSink) *StorageService {
	rc := StorageService{
		database:  database,
		queue:     queue,
		blobStore: blobStore,
		dataSink:  dataSink,
	}
	return &rc
}
