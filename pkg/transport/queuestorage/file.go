package queuestorage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
)

type FileEvent struct {
	Key  string
	Path string
}

type FileWriter struct {
	key  string
	path string

	store  filestore.StorageBackend
	queue  queue.QueueBackend
	notify chan FileEvent

	maxFileSize int64
	maxRows     int64
	expiry      time.Time

	// Current file being written to
	fd *os.File

	// Ensure only 1 file write (or rotate) is happening at a time
	canWrite sync.Mutex

	// Used to rotate every x interval
	ticker *time.Ticker

	wg sync.WaitGroup
}

type NewFileWriterParam struct {
	Key    string
	Path   string
	Store  filestore.StorageBackend
	Queue  queue.QueueBackend
	Notify chan FileEvent

	MaxFileSize int64
	MaxRows     int64
	MaxFileAge  time.Duration
}

func NewFileWriter(param NewFileWriterParam) (*FileWriter, error) {
	fw := &FileWriter{
		key:         param.Key,
		path:        param.Path,
		store:       param.Store,
		queue:       param.Queue,
		notify:      param.Notify,
		maxFileSize: param.MaxFileSize,
		maxRows:     param.MaxRows,

		ticker: time.NewTicker(time.Second),

		expiry: time.Now().Add(param.MaxFileAge),
	}

	if fw.notify == nil {
		return nil, fmt.Errorf("notify channel cannot be nil")
	}

	if err := fw.create(); err != nil {
		return nil, err
	}

	return fw, nil
}

func (f *FileWriter) create() error {
	err := os.MkdirAll(filepath.Dir(f.path), os.ModePerm)
	if err != nil {
		log.Err(err).Msg("Unable to create all directories in file path")
		return err
	}

	f.fd, err = os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Err(err).Send()
		return err
	}
	_ = f.fd.SetDeadline(f.expiry)

	return nil
}

func (f *FileWriter) countDown() {
	defer f.wg.Done()
	defer f.ticker.Stop()

	for {
		select {
		case now := <-f.ticker.C:
			if now.After(f.expiry) {
				log.Info().Str("key", f.key).
					Str("filePath", f.path).
					Msg("Maximum file age reached, closing file")
				err := f.Close()
				if err != nil {
					log.Err(err).Send()
				}
				return
			}
		}
	}
}

func (f *FileWriter) Write(data []byte) (n int, err error) {
	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	// check to see if we will hit our file size limit
	fileInfo, err := f.fd.Stat()
	if err != nil {
		return
	}

	if fileInfo.Size()+int64(len(data)) > f.maxFileSize {
		if err := f.Close(); err != nil {
			log.Err(err).Send()
		}
		err = fmt.Errorf("file size limit reached")
		return
	}

	// write data
	if n, err = f.fd.Write(data); err != nil {
		log.Err(err).Send()
		return
	}

	return
}

func (f *FileWriter) WriteLn(data []byte) (n int, err error) {
	return f.Write(append(data, '\n'))
}

func (f *FileWriter) Close() error {
	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("Closing file")
	f.notify <- FileEvent{Key: f.key, Path: f.path}

	if err := f.fd.Close(); err != nil {
		return err
	}
	f.wg.Wait()

	return nil
}
