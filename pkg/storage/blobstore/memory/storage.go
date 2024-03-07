package memory

import (
	"fmt"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/models"
	"io"
	"sync"
)

type Storage struct {
	mu    sync.RWMutex
	items map[string][]byte
}

func (s *Storage) Upload(path string, r io.ReadSeeker) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[path] = data

	return nil
}

func (s *Storage) Download(path string, w io.WriterAt) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.items[path]
	if !ok {
		return models.ErrNotFound
	}
	if _, err := w.WriteAt(data, 0); err != nil {
		return fmt.Errorf("Storage.Download: %s: %w", path, err)
	}
	return nil
}

// NewStorage returns a new initialized Storage
func NewStorage(conf config.BlobStore) (*Storage, error) {
	rc := &Storage{
		items: map[string][]byte{},
	}
	return rc, nil
}
