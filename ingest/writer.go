package ingest

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type FileWriter struct {
	// Where to save data
	DataDirectory string
	// How often to rotate log file
	MaxAgeSeconds int
	// Max file size before rotating
	MaxSizeBytes int

	// Current file being written to
	// currentFilename string
	fd *os.File

	// Previous file which needs to be rotated
	previousFiles []string

	// How many bytes have been sent with current file
	bytesSent int
	// When file was last rotated
	lastRotated time.Time

	rotating sync.Mutex
	canWrite sync.Mutex
	// performWrite sync.Mutex
}

func NewFileWriter(DataDirectory string, MaxAgeSeconds int, MaxSizeBytes int) *FileWriter {
	fw := &FileWriter{
		DataDirectory: DataDirectory,
		MaxAgeSeconds: MaxAgeSeconds,
		MaxSizeBytes:  MaxSizeBytes,
		previousFiles: make([]string, 0),
	}

	return fw
}

// TODO: only upload based on timer if there is actually data

func (f *FileWriter) Rotate() error {
	// Make sure only one rotation is happening at a time
	rotating := f.rotating.TryLock()
	if !rotating {
		log.Println("Someone else is currently rotating, skipping this rotation")
		return nil
	}
	defer f.rotating.Unlock()

	log.Println("Rotating!")
	var err error

	// BLOCK ALL WRITES while we rotate.
	// We can probalby be more clever here by opening the new file
	// to continue writes while we close the previous
	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	// Check to see if we have an open fd
	if f.fd != nil {
		// Close file descriptor
		err = f.fd.Close()

		// Unable to close file!
		if err != nil {
			log.Println(err)
			return err
		}
		f.previousFiles = append(f.previousFiles, f.fd.Name())
	}

	newFileId := ulid.Make().String()
	dir := filepath.Join(f.DataDirectory)
	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Println(err)
		return err
	}

	path := filepath.Join(f.DataDirectory, newFileId+".ndjson")

	f.fd, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		return err
	}

	f.bytesSent = 0

	return nil
}

func (f *FileWriter) Write(data string) error {
	var err error

	// check if there is a file, if not, create one
	if f.fd == nil {
		// log.Println("No file, creating one")
		err = f.Rotate()
	}
	if err != nil {
		log.Println(err)
		return err
	}

	// check to see if we will hit our file size limit
	if f.bytesSent+len(data) > f.MaxSizeBytes {
		// log.Println("Max size reached, rotating")
		err = f.Rotate()
	}
	if err != nil {
		log.Println(err)
		return err
	}

	// write data
	f.canWrite.Lock()
	n, err := f.fd.WriteString(data + "\n")
	if err == nil {
		f.bytesSent += n
	} else {
		log.Println(err)
	}
	f.canWrite.Unlock()

	return err
}

func (f *FileWriter) Close() error {
	// Close open file
	err := f.Rotate()

	// Check on this error
	if err != nil {
		log.Println(err)
	}

	log.Println("Finishing uploading files")

	return nil
}
