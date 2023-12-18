package dummy

import (
	"errors"
	"io"
)

type DummyStorage struct {
}

func (s *DummyStorage) Upload(path string, r io.ReadSeeker) error {
	return errors.New("Upload not implemented for dummy storage")
}

func (s *DummyStorage) Download(path string, w io.WriterAt) error {
	return errors.New("Download not implemented for dummy storage")
}
