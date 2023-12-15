package dummy

import (
	"errors"
	"io"
)

type DummyStorage struct {
}

func (s *DummyStorage) Upload(path string, r io.Reader) error {
	return errors.New("Upload not implemented for dummy storage")
}

func (s *DummyStorage) Download(path string) (io.ReadCloser, error) {
	return nil, errors.New("Download not implemented for dummy storage")
}
