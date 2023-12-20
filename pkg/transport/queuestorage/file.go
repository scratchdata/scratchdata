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

type FileWriterParam struct {
	Key   string
	Dir   string
	Table string

	MaxFileSize int64
	MaxRows     int64
	MaxFileAge  time.Duration

	Queue   queue.QueueBackend
	Storage filestore.StorageBackend
}

type FileWriterInfo struct {
	Key    string
	Path   string
	Table  string
	Closed bool
}

type FileWriter struct {
	key   string
	path  string
	table string

	maxFileSize int64
	maxRows     int64
	maxFileAge  time.Duration

	queue   queue.QueueBackend
	storage filestore.StorageBackend

	// fd is the file descriptor of the target file
	fd *os.File

	// mu ensure a sequential file change operation
	mu sync.Mutex

	// terminated is a flag to indicate the file writer is terminated.
	// When true, all write operations will return a non-nil error
	// and file rotations will stop.
	terminated bool

	// timer monitors the current file until maxFileAge is reached
	timer *time.Timer
}

func NewFileWriter(param FileWriterParam) (*FileWriter, error) {
	errMsgTmpl := "%s should be a number greater than zero"
	if param.MaxFileSize == 0 {
		return nil, fmt.Errorf(errMsgTmpl, "MaxFileSize")
	}
	if param.MaxRows == 0 {
		return nil, fmt.Errorf(errMsgTmpl, "MaxRows")
	}
	if param.MaxFileAge == 0 {
		return nil, fmt.Errorf(errMsgTmpl, "MaxFileAge")
	}

	fw := &FileWriter{
		key:         param.Key,
		table:       param.Table,
		maxFileSize: param.MaxFileSize,
		maxRows:     param.MaxRows,
		maxFileAge:  param.MaxFileAge,

		queue:   param.Queue,
		storage: param.Storage,
	}

	fileName := filepath.Join(param.Dir, fw.key, ulid.Make().String()+".ndjson")
	if err := fw.create(fileName); err != nil {
		return nil, err
	}

	return fw, nil
}

// create creates a file and all directories in its path.
// It sets a timer which triggers close after the maxFileAge elapses.
func (f *FileWriter) create(fileName string) error {
	f.path = fileName
	err := os.MkdirAll(filepath.Dir(f.path), os.ModePerm)
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

// ensureWritable checks for constraints before any write operation.
func (f *FileWriter) ensureWritable(dataSize int64) error {
	var fileInfo os.FileInfo
	fileInfo, err := f.fd.Stat()
	if err != nil {
		return err
	}
	newSize := fileInfo.Size() + dataSize

	rowLimit := f.maxRows <= 0
	sizeLimit := newSize > f.maxFileSize
	if rowLimit || sizeLimit {
		return f.close(false)
	}

	return nil
}

// postOps uploads the current file and queues its detail
func (f *FileWriter) postOps() error {
	log.Info().
		Str("key", f.key).
		Str("filePath", f.path).
		Msg("uploading file")

	file, err := os.Open(f.path)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Err(err).
				Str("filePath", f.path).
				Msg("failed to close file in postOps")
		}
	}(file)
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
		Key:   f.key,
		Path:  f.path,
		Table: f.table,
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

	return nil
}

// Write writes a line of data to the file. It returns the number
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

// Info returns the current file detail
func (f *FileWriter) Info() FileWriterInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return FileWriterInfo{
		Key:    f.key,
		Path:   f.path,
		Table:  f.table,
		Closed: f.terminated,
	}
}

// rotate starts a new file when limits are reached.
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
