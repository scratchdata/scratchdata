package storage

import (
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore"
	"github.com/scratchdata/scratchdata/pkg/storage/cache"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
	"github.com/scratchdata/scratchdata/pkg/storage/queue"
)

type Services struct {
	Database  database.Database
	Cache     cache.Cache
	Queue     queue.Queue
	BlobStore blobstore.BlobStore
}

func New(c config.ScratchDataConfig) (*Services, error) {
	rc := &Services{}

	var err error
	if rc.BlobStore, err = blobstore.NewBlobStore(c.BlobStore); err != nil {
		return nil, err
	}

	if rc.Queue, err = queue.NewQueue(c.Queue); err != nil {
		return nil, err
	}

	if rc.Cache, err = cache.NewCache(c.Cache); err != nil {
		return nil, err
	}

	if rc.Database, err = database.NewGorm(c.Database, c.Destinations, c.APIKeys); err != nil {
		return nil, err
	}

	return rc, nil
}
