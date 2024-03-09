package filesystem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/EagleChen/mapmutex"
	"github.com/bwmarrin/snowflake"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/util"
)

const OpenFolder = "open"
const ClosedFolder = "closed"

type DataSink struct {
	DataDir           string `mapstructure:"data"`
	MaxFileSize       int64  `mapstructure:"max_size_bytes"`
	MaxRows           int64  `mapstructure:"max_rows"`
	MaxFileAgeSeconds int    `mapstructure:"max_age_seconds"`

	storage *models.StorageServices
	snow    *snowflake.Node
	enabled bool
	wg      sync.WaitGroup

	fileMutex *mapmutex.Mutex
	files     map[string]*FileDetails

	//uploadQueue []*FileDetails
}

type FileDetails struct {
	fd        *os.File
	path      string
	rowCount  int64
	byteCount int64
	created   time.Time

	databaseId int64
	table      string
}

func (d *FileDetails) Directory() string {
	return filepath.Dir(d.path)
}

func (d *FileDetails) Name() string {
	return filepath.Base(d.path)
}

func (m *DataSink) Start(ctx context.Context) error {
	m.enabled = true

	m.wg.Add(1)
	go m.MonitorFiles(ctx)

	<-ctx.Done()
	return m.Shutdown()
}

func (m *DataSink) RotateAllFiles(forceRotation bool, createNew bool) {
	for key := range m.files {
		if m.fileMutex.TryLock(key) {
			fileDetails, ok := m.files[key]
			if fileDetails != nil && ok {
				if m.NeedsRotation(fileDetails) || forceRotation {
					log.Trace().Str("file", fileDetails.path).Msg("Rotating")
					_, err := m.RotateFile(fileDetails, createNew)
					if err != nil {
						log.Error().Err(err).Str("file", fileDetails.path).Msg("Unable to auto-rotate file")
					}
				}
			}
			m.fileMutex.Unlock(key)
		}
	}
}

func (m *DataSink) MonitorFiles(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.RotateAllFiles(false, true)
			log.Trace().Msg("tick")
		case <-ctx.Done():
			log.Trace().Msg("Stopping rotation")
			return
		}
	}
}

func (m *DataSink) NeedsRotation(details *FileDetails) bool {
	if details.byteCount >= m.MaxFileSize {
		return true
	}

	if details.rowCount >= m.MaxRows {
		return true
	}

	if details.byteCount > 0 && time.Now().Sub(details.created) >= time.Duration(time.Second*time.Duration(m.MaxFileAgeSeconds)) {
		return true
	}

	return false
}

func (m *DataSink) RotateFile(details *FileDetails, createNew bool) (*FileDetails, error) {
	key := m.key(details.databaseId, details.table)

	err := details.fd.Close()
	if err != nil {
		return nil, err
	}

	delete(m.files, key)

	if details.byteCount > 0 {
		closedFolderPath := filepath.Join(m.DataDir, ClosedFolder, fmt.Sprintf("%d", details.databaseId), details.table)
		err = os.MkdirAll(closedFolderPath, os.ModePerm)
		if err != nil {
			return nil, err
		}

		closedPath := filepath.Join(closedFolderPath, details.Name())
		err = os.Link(details.path, closedPath)
		if err != nil {
			return nil, err
		}
	}

	err = os.Remove(details.path)
	if err != nil {
		log.Error().Err(err).Int64("database", details.databaseId).Str("table", details.table).Str("path", details.path).Msg("Unable to delete zombie file. Has been moved to the closed dir.")
	}

	if createNew {
		newFile, err := m.CreateFile(details.databaseId, details.table)
		if err != nil {
			return nil, err
		}

		m.files[key] = newFile
		return newFile, nil
	}

	return nil, nil
}

func (m *DataSink) IsDiskFull() (bool, error) {
	return false, nil
}

func (m *DataSink) CreateFile(databaseID int64, table string) (*FileDetails, error) {
	var fd *os.File
	var err error

	fileSnowflake := m.snow.Generate()
	tableDir := filepath.Join(m.DataDir, OpenFolder, fmt.Sprintf("%d", databaseID), table)
	fileName := fmt.Sprintf("%s.ndjson", fileSnowflake.String())

	err = os.MkdirAll(tableDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(tableDir, fileName)
	fd, err = os.Create(filePath)
	if err != nil {
		return nil, err
	}

	fileDetails := &FileDetails{
		fd:      fd,
		path:    filePath,
		created: time.Now(),

		databaseId: databaseID,
		table:      table,
	}

	return fileDetails, nil
}

func (m *DataSink) EnsureFile(databaseID int64, table string) (*FileDetails, error) {
	key := m.key(databaseID, table)

	var fileDetails *FileDetails
	var err error

	// If the file doesn't exist, then create it
	fileDetails, ok := m.files[key]
	if !ok {
		fileDetails, err = m.CreateFile(databaseID, table)
		if err != nil {
			return nil, err
		}

		m.files[key] = fileDetails
		return fileDetails, nil
	}

	needsRotation := m.NeedsRotation(fileDetails)
	if needsRotation {
		fileDetails, err = m.RotateFile(fileDetails, true)
	}
	return fileDetails, err
}

func (m *DataSink) key(databaseID int64, table string) string {
	return fmt.Sprintf("%d_%s", databaseID, table)
}

func (m *DataSink) WriteData(databaseID int64, table string, data []byte) error {
	if !m.enabled {
		return errors.New("writer is disabled")
	}

	m.wg.Add(1)
	defer m.wg.Done()

	// TODO: Is the disk full?
	isFull, err := m.IsDiskFull()
	if err != nil {
		return err
	}
	if isFull {
		return errors.New("Disk is full")
	}

	mutexKey := m.key(databaseID, table)
	if m.fileMutex.TryLock(mutexKey) {
		defer m.fileMutex.Unlock(mutexKey)

		fileDetails, err := m.EnsureFile(databaseID, table)
		if err != nil {
			return err
		}

		bytesWritten, err := fileDetails.fd.Write(data)
		if err != nil {
			return err
		}
		fileDetails.byteCount += int64(bytesWritten)

		bytesWritten, err = fileDetails.fd.Write([]byte("\n"))
		if err != nil {
			return err
		}
		fileDetails.byteCount += int64(bytesWritten)

		fileDetails.rowCount += 1
	} else {
		return errors.New("Could not acquire lock")
	}

	// Ensure there's a file to write to
	// Write to it

	// In the background: rotate and upload files

	//reader := bytes.NewReader(data)
	//
	//uploadErr := m.storage.BlobStore.Upload(key, reader)
	//if uploadErr != nil {
	//	return uploadErr
	//}
	//
	//uploadMessage := queue_models.FileUploadMessage{
	//	DatabaseID: databaseID,
	//	Table:      table,
	//	Key:        key,
	//}
	//
	//// TODO: log payload for replay
	//message, err := json.Marshal(uploadMessage)
	//if err != nil {
	//	return err
	//}
	//
	//// TODO: log payload for replay
	//err = m.storage.Queue.Enqueue(message)
	//if err != nil {
	//	return err
	//}

	return nil
}

func (m *DataSink) Shutdown() error {
	m.enabled = false
	m.wg.Wait()

	m.RotateAllFiles(true, false)

	return nil
}

func NewFilesystemDataSink(settings map[string]any, storage *models.StorageServices) (*DataSink, error) {
	rc := util.ConfigToStruct[DataSink](settings)

	openDir := filepath.Join(rc.DataDir, OpenFolder)
	closedDir := filepath.Join(rc.DataDir, ClosedFolder)

	err := os.MkdirAll(openDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(closedDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	snow, err := util.NewSnowflakeGenerator()
	if err != nil {
		return nil, err
	}

	rc.storage = storage
	rc.snow = snow
	rc.fileMutex = mapmutex.NewMapMutex()
	rc.files = map[string]*FileDetails{}

	return rc, nil
}
