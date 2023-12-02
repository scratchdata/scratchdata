package storage

import "io"

type StorageBackend interface {
	Upload(path string, reader io.Reader) error
	Download(path string) io.Writer
}
