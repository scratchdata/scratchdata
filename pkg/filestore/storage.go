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
	// Upload stores the content of r at path
	Upload(path string, r io.ReadSeeker) error

	// Download fetches the content stored at path and writes it to w or returns an error
	//
	// If path is not found (wrapped) error ErrNotFound is returned
	Download(path string, w io.WriterAt) error
}
