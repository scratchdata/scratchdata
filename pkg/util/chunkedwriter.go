package util

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type ChunkedWriter struct {
	maxSize   int
	chunkSize int
	folder    string

	currentChunkSize int
	currentSize      int
	fileCount        int

	mu *sync.Mutex

	bufioSize int
	writer    *bufio.Writer

	fd *os.File
}

func NewChunkedWriter(maxSize int, chunkSize int, folder string) *ChunkedWriter {
	return &ChunkedWriter{
		maxSize:   maxSize,
		chunkSize: chunkSize,
		folder:    folder,

		bufioSize: 1024 * 1024 * 10, // 10 MB
		mu:        &sync.Mutex{},
	}
}

func (w *ChunkedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(data)+w.currentSize > w.maxSize {
		return 0, errors.New("file size exceeded")
	}

	// accumulator of how much data we've written during this call
	var written int

	// position of buffer when writing lines
	var writeUpTo int

	for {
		// There is no file, so create one
		if w.fd == nil {
			fileName := fmt.Sprintf("file-%d", w.fileCount)
			path := filepath.Join(w.folder, fileName)

			fd, err := os.Create(path)
			if err != nil {
				return 0, err
			}
			w.fd = fd

			w.writer = bufio.NewWriterSize(w.fd, w.bufioSize)
		}

		// By default, write all data
		writeUpTo = len(data)

		// Check for newline character and write one line at a time
		newlinePosition := bytes.IndexByte(data, '\n')
		if newlinePosition > -1 {
			writeUpTo = newlinePosition + 1
		}

		// write data
		n, err := w.writer.Write(data[:writeUpTo])
		if err != nil {
			return 0, err
		}

		// Increment amount of data we've written, amount for this chunk, and total for writer
		written += n
		w.currentChunkSize += n
		w.currentSize += n

		// if the current chunk is at capacity and there is another line available
		// then close the current file
		if w.currentChunkSize >= w.chunkSize && newlinePosition > -1 {
			err = w.writer.Flush()
			if err != nil {
				return 0, err
			}

			err = w.fd.Close()
			if err != nil {
				return 0, err
			}

			w.fileCount++
			w.currentChunkSize = 0

			w.writer = nil
			w.fd = nil
		}

		data = data[writeUpTo:]

		if len(data) == 0 {
			break
		}

	}

	return written, nil
}

func (w *ChunkedWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		err := w.writer.Flush()
		if err != nil {
			return err
		}
	}

	if w.fd != nil {
		err := w.fd.Close()
		if err != nil {
			return err
		}
	}

	return nil
}
