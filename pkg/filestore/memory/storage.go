package memory

import (
	"fmt"
	"io"
	"scratchdata/pkg/filestore"
	"sync"
)

var (
	_ filestore.StorageBackend = (*Storage)(nil)
)

// Storage implements an in-memory filestore.StorageBackend
type Storage struct {
	mu   sync.Mutex
	ents map[string][]byte
}

// Upload implements filestore.StorageBackend.Upload
func (s *Storage) Upload(path string, r io.ReadSeeker) error {
	// copy message to avoid external modification
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Storage.Upload: %s: %w", path, filestore.ErrNotFound)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ents == nil {
		s.ents = map[string][]byte{}
	}
	s.ents[path] = data

	return nil
}

// Download implements filestore.StorageBackend.Download
func (s *Storage) Download(path string, w io.WriterAt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, ok := s.ents[path]
	if !ok {
		return fmt.Errorf("Storage.Download: %s: %w", path, filestore.ErrNotFound)
	}
	if _, err := w.WriteAt(data, 0); err != nil {
		return fmt.Errorf("Storage.Download: %s: %w", path, err)
	}
	return nil
}

// NewStorage returns a new initialized Storage
func NewStorage() *Storage {
	return &Storage{}
}
