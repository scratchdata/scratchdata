package mocks

import (
	"io"

	"github.com/stretchr/testify/mock"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
)

type TestStorage struct {
	filestore.StorageBackend
	mock.Mock
}

func (t *TestStorage) Upload(path string, reader io.Reader) error {
	args := t.Called(path, reader)
	return args.Error(0)
}

func (t *TestStorage) Download(path string) io.Writer {
	args := t.Called(path)
	return args.Get(0).(io.Writer)
}

type TestQueue struct {
	queue.QueueBackend
	mock.Mock
}

func (t *TestQueue) Enqueue(message []byte) error {
	args := t.Called(message)
	return args.Error(0)
}

func (t *TestQueue) Dequeue() ([]byte, error) {
	args := t.Called()
	return args.Get(0).([]byte), args.Error(1)
}
