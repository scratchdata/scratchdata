package queuestorage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/sjson"
	"scratchdata/models"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
)

const (
	MaxFileSize int64 = 100 * 1024 * 1024 // 100MB
	MaxRows     int64 = 1_000
	MaxFileAge        = 1 * time.Hour
)

type FileWriterParam struct {
	Key string
	Dir string

	MaxFileSize int64
	MaxRows     int64
	MaxFileAge  time.Duration

	Queue   queue.QueueBackend
	Storage filestore.StorageBackend
}

type FileWriterInfo struct {
	Key    string
	Path   string
	Closed bool
}

type FileWriter struct {
	key  string
	path string

	maxFileSize int64
	maxRows     int64
	maxFileAge  time.Duration

	queue   queue.QueueBackend
	storage filestore.StorageBackend

	// fd is the file descriptor of the target file
	fd *os.File

	// mu ensure a sequential file write operation
	mu sync.Mutex

	// terminated is a flag to indicate the file writer is terminated
	terminated bool

	timer *time.Timer
}

func NewFileWriter(param FileWriterParam) (*FileWriter, error) {
	if param.MaxFileSize == 0 {
		param.MaxFileSize = MaxFileSize
	}
	if param.MaxRows == 0 {
		param.MaxRows = MaxRows
	}
	if param.MaxFileAge == 0 {
		param.MaxFileAge = MaxFileAge
	}

	fileName := filepath.Join(param.Dir, ulid.Make().String()+".ndjson")
	fw := &FileWriter{
		key: param.Key,
		//path:        filepath.Join(param.Dir, fileName),
		maxFileSize: param.MaxFileSize,
		maxRows:     param.MaxRows,
		maxFileAge:  param.MaxFileAge,

		queue:   param.Queue,
		storage: param.Storage,
	}

	if err := fw.create(fileName); err != nil {
		return nil, err
	}

	return fw, nil
}

func (f *FileWriter) create(fileName string) error {
	f.path = fileName
	dir := filepath.Dir(f.path)
	err := os.MkdirAll(filepath.Join(dir, f.key), os.ModePerm)
	if err != nil {
		log.Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("unable to create all directories in file path")
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

	f.timer = time.AfterFunc(f.maxFileAge, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if !f.terminated {
			log.Info().
				Str("key", f.key).
				Str("filePath", f.path).
				Msg("maximum file age reached, closing file")

			if err := f.close(false); err != nil {
				log.Err(err).
					Str("key", f.key).
					Str("filePath", f.path).
					Msg("ticker unable to close file")
			}
		}
	})

	return nil
}

func (f *FileWriter) ensureWritable(dataSize int64) (err error) {
	var openNew bool
	defer func() {
		if openNew {
			err = f.close(false)
		}
	}()

	// check to see if we will hit our row limit
	if f.maxRows <= 0 {
		openNew = true
		return
	}

	// check to see if we will hit our file size limit
	var fileInfo os.FileInfo
	fileInfo, err = f.fd.Stat()
	if err != nil {
		return
	}

	newSize := fileInfo.Size() + dataSize
	if newSize > f.maxFileSize {
		openNew = true
		return
	}

	return
}

func (f *FileWriter) postOps() error {
	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("uploading file")

	file, err := os.Open(f.path)
	if err != nil {
		log.Error().Err(err).
			Str("filePath", f.path).
			Msg("unable to open file for upload")
		return err
	}

	if err := f.storage.Upload(f.path, file); err != nil {
		log.Error().Err(err).
			Str("filePath", f.path).
			Msg("unable to upload file")
		return err
	}

	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("queuing file")

	var bb []byte
	if bb, err = json.Marshal(models.FileUploadMessage{
		Key:  f.key,
		Path: f.path,
	}); err != nil {
		log.Error().Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("unable to marshal file upload message")
		return err
	}

	if err := f.queue.Enqueue(bb); err != nil {
		log.Error().Err(err).
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("unable to enqueue file")
		return err
	}

	// TODO: Consider removing file after upload
	// TODO:

	return nil
}

// Write writes a line of data to the file. t returns the number
// of bytes written and an error, if any. Write returns
// a non-nil error when n != len(b) or a constraint is unmet.
func (f *FileWriter) Write(data []byte) (n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.terminated {
		return 0, fmt.Errorf("file writer is terminated")
	}

	rowID := ulid.Make().String()
	if data, err = sjson.SetBytes(data, "__row_id", rowID); err != nil {
		log.Err(err).Msg("unable to set __row_id in JSON")
	}
	if data, err = sjson.SetBytes(data, "__batch_file", f.path); err != nil {
		log.Err(err).Msg("unable to set __batch_file in JSON")
	}

	data = append(data, '\n')

	dataSize := int64(len(data))
	if dataSize > f.maxFileSize {
		return 0, fmt.Errorf("data size %d exceeds maximum file size %d", dataSize, f.maxFileSize)
	}

	if err := f.ensureWritable(dataSize); err != nil {
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

func (f *FileWriter) Info() FileWriterInfo {
	return FileWriterInfo{
		Key:    f.key,
		Path:   f.path,
		Closed: f.terminated,
	}
}

func (f *FileWriter) rotate() error {
	log.Info().Str("key", f.key).
		Str("filePath", f.path).
		Msg("rotating file")

	if err := f.fd.Close(); err != nil {
		return err
	}

	if f.terminated {
		return nil
	}

	dataDir := filepath.Dir(f.path)
	fileName := filepath.Join(dataDir, ulid.Make().String()+".ndjson")

	return f.create(fileName)
}

func (f *FileWriter) close(terminate bool) error {
	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("closing file")

	f.terminated = terminate

	if ok := f.timer.Stop(); !ok {
		log.Error().
			Str("key", f.key).
			Str("filePath", f.path).
			Msg("timer already stopped")
	}

	// TODO: Do not upload/push empty files

	if err := f.postOps(); err != nil {
		return err
	}

	if err := f.rotate(); err != nil {
		return err
	}
	return nil
}

// Close closes the file descriptor and stops all processes.
// It sends the FileInfo to the notify channel and stops
// the ticker process. The receiver blocks if the notify
// channel is full.
func (f *FileWriter) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.close(true)
}
