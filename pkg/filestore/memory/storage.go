package memory

import (
	"bytes"
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
func (s *Storage) Upload(path string, r io.Reader) error {
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
func (s *Storage) Download(path string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if data, ok := s.ents[path]; ok {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return nil, fmt.Errorf("Storage.Download: %s: %w", path, filestore.ErrNotFound)
}

// NewStorage returns a new initialized Storage
func NewStorage() *Storage {
	return &Storage{}
}
