package memory

import (
	"fmt"
	"github.com/scratchdata/scratchdata/config"
	"io"
	"sync"

	"github.com/scratchdata/scratchdata/pkg/filestore"
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
	defer s.mu.Unlock()

	data, ok := s.items[path]
	if !ok {
		return filestore.ErrNotFound
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
