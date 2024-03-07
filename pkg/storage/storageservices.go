package storage

import (
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
)

type StorageServices interface {
	Database() database.Database
	Queue() QueueI
	Cache() CacheI
	BlobStore() BlobStoreI
	DataSink() DataSinkI
	Destination(databaseID int64) destinations.Destination
}

type QueueI interface{}
type CacheI interface{}
type BlobStoreI interface{}
type DataSinkI interface {
	WriteData(databaseID int64, table string, data []byte) error
}

type StorageService struct {
	database  database.Database
	cache     CacheI
	queue     QueueI
	blobStore BlobStoreI
	dataSink  DataSinkI
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

func (s *StorageService) DataSink() DataSinkI {
	return s.dataSink
}

func (s *StorageService) Destination(databaseID int64) destinations.Destination {
	// todo: get creds
	// create db connection object
	// possibly cache/pool it
	return nil
}

func NewStorageService(database database.Database, cache CacheI, queue QueueI, blobStore BlobStoreI, dataSink DataSinkI) *StorageService {
	rc := StorageService{}
	return &rc
}
