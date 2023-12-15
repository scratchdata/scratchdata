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

const (
	MaxFileSize int64 = 100 * 1024 * 1024 // 100MB
	MaxRows     int64 = 1_000
	MaxFileAge        = 1 * time.Hour
)

type NewFileWriterParam struct {
	Key    string
	Path   string
	Store  filestore.StorageBackend
	Queue  queue.QueueBackend
	Notify chan FileWriterInfo

	MaxFileSize int64
	MaxRows     int64
	Expiry      time.Time
}

type FileWriterInfo struct {
	Key         string
	Path        string
	MaxFileSize int64
	MaxRows     int64
	Expiry      time.Time
}

type FileWriter struct {
	key  string
	path string

	store  filestore.StorageBackend
	queue  queue.QueueBackend
	notify chan FileWriterInfo

	maxFileSize int64
	maxRows     int64
	expiry      time.Time

	// fd is the file descriptor of the target file
	fd *os.File

	// canWrite ensure a sequential file write operation
	canWrite sync.Mutex

	// stopTicker signals for the stoppage of file age ticker
	stopTicker chan struct{}
}

func NewFileWriter(param NewFileWriterParam) (*FileWriter, error) {
	if param.MaxFileSize == 0 {
		param.MaxFileSize = MaxFileSize
	}
	if param.MaxRows == 0 {
		param.MaxRows = MaxRows
	}

	fw := &FileWriter{
		key:         param.Key,
		path:        param.Path,
		store:       param.Store,
		queue:       param.Queue,
		notify:      param.Notify,
		maxFileSize: param.MaxFileSize,
		maxRows:     param.MaxRows,
		expiry:      param.Expiry,

		stopTicker: make(chan struct{}),
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
		log.Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("Unable to create all directories in file path")
		return err
	}

	f.fd, err = os.OpenFile(f.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Send()
		return err
	}
	err = f.fd.SetDeadline(f.expiry)
	log.Warn().Err(err).
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("failed to set deadline")

	go f.countDown()

	return nil
}

func (f *FileWriter) countDown() {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case now := <-ticker.C:
			if now.After(f.expiry) {
				log.Info().Str("key", f.key).
					Str("filePath", f.path).
					Msg("Maximum file age reached, closing file")

				if err := f.Close(); err != nil {
					log.Err(err).
						Str("key", f.key).
						Str("filePath", f.path).
						Msg("ticker unable to close file")
				}
				return
			}
		case <-f.stopTicker:
			log.Info().
				Str("key", f.key).
				Str("filePath", f.path).
				Msg("stop ticker signal received")
			return
		}
	}
}

func (f *FileWriter) checkWriteConstraints(dataSize int64) error {
	// check to see if we have a file open
	if f.fd == nil {
		return fmt.Errorf("file has been closed")
	}

	// check to see if we will hit our row limit
	if f.maxRows <= 0 {
		if err := f.Close(); err != nil {
			log.Err(err).Send()
		}
		return fmt.Errorf("file size limit reached")
	}

	// check to see if we will hit our file size limit
	fileInfo, err := f.fd.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+dataSize > f.maxFileSize {
		if err := f.Close(); err != nil {
			log.Err(err).Send()
		}
		return fmt.Errorf("file size limit reached")
	}

	return nil
}

// Write writes data to the file. t returns the number
// of bytes written and an error, if any. Write returns
// a non-nil error when n != len(b) or a constraint is unmet.
func (f *FileWriter) Write(data []byte) (n int, err error) {
	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	dataSize := int64(len(data))
	if err := f.checkWriteConstraints(dataSize); err != nil {
		return 0, err
	}

	// write data
	if n, err = f.fd.Write(data); err != nil {
		log.Err(err).Send()
		return
	}

	f.maxRows--
	return
}

// WriteLn writes data to the file followed by a newline. It uses Write internally.
func (f *FileWriter) WriteLn(data []byte) (n int, err error) {
	if n, err = f.Write(data); err != nil {
		return
	}

	nn, err := f.Write([]byte{'\n'})
	n = n + nn
	return
}

func (f *FileWriter) Info() FileWriterInfo {
	return FileWriterInfo{
		Key:         f.key,
		Path:        f.path,
		MaxFileSize: f.maxFileSize,
		MaxRows:     f.maxRows,
		Expiry:      f.expiry,
	}
}

// Close closes the file descriptor and stops all processes.
// It sends the FileInfo to the notify channel and stops
// the ticker process. The receiver blocks if the notify
// channel is full.
func (f *FileWriter) Close() error {
	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("Closing file")
	f.notify <- f.Info()

	// stop ticker
	close(f.stopTicker)

	err := f.fd.Close()
	if err != nil {
		log.Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("failed to close file")
	}
	f.fd = nil

	return nil
}
