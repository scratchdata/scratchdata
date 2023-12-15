package filestore

import (
	"errors"
	"io"
)

var (
	ErrNotFound = errors.New("not found")
)

// StorageBackend is the interface implemented by storage backends
type StorageBackend interface {
	// Upload stores the content of reader at path
	Upload(path string, reader io.Reader) error

	// Download fetches the content stored at path or returns an error
	//
	// If path is not found (wrapped) error ErrNotFound is returned
	Download(path string) (io.ReadCloser, error)
}
