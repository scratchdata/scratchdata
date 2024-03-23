package models

import (
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore"
	"github.com/scratchdata/scratchdata/pkg/storage/cache"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
	"github.com/scratchdata/scratchdata/pkg/storage/queue"
	"github.com/scratchdata/scratchdata/pkg/storage/vault"
)

type StorageServices struct {
	Database  database.Database
	Cache     cache.Cache
	Queue     queue.Queue
	BlobStore blobstore.BlobStore
	Vault     vault.Vault
}
