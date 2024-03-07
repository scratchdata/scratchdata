package models

import (
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore"
	"github.com/scratchdata/scratchdata/pkg/storage/cache"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
	"github.com/scratchdata/scratchdata/pkg/storage/queue"
)

type Permission string

const (
	Read  Permission = "read"
	Write Permission = "write"
)

type Account struct {
	ID string `toml:"id"`
}

type User struct {
	ID         string   `toml:"id"`
	AccountIDs []string `toml:"accounts"`
}

type DatabaseConnection struct {
	ID                 string                 `toml:"id"`
	AccountID          string                 `toml:"account_id"`
	Permissions        []Permission           `toml:"permissions"`
	Type               string                 `toml:"type"`
	ConnectionSettings map[string]interface{} `toml:"settings"`
}

type FileUploadMessageOld struct {
	// Key is the unique database connection identifier
	Key string `json:"key"`

	// Path is the storage upload key.
	Path string `json:"path"`

	// Table is the database table name which the data represents
	Table string `json:"table"`
}

type StorageServices struct {
	Database  database.Database
	Cache     cache.Cache
	Queue     queue.Queue
	BlobStore blobstore.BlobStore
	//DataSink  datasink.DataSink
}
