package blobstore

import (
	"errors"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/memory"
	"io"
)

type BlobStore interface {
	Upload(path string, r io.ReadSeeker) error
	Download(path string, w io.WriterAt) error
}

func NewBlobStore(conf config.BlobStore) (BlobStore, error) {
	switch conf.Type {
	case "memory":
		return memory.NewStorage(conf)
	}

	return nil, errors.New("Unsupported blob store")
}
