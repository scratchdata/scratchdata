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
	MaxSizeBytes int64

	// Current file being written to
	fd *os.File

	// Previous file which needs to be rotated
	previousFiles []string

	// Ensure only 1 rotation is happening at a time
	rotating sync.Mutex
	// Ensure only 1 file write (or rotate) is happening at a time
	canWrite sync.Mutex

	// Used to rotate every x interval
	ticker     *time.Ticker
	tickerDone chan bool
}

func NewFileWriter(DataDirectory string, MaxAgeSeconds int, MaxSizeBytes int64) *FileWriter {
	fw := &FileWriter{
		DataDirectory: DataDirectory,
		MaxAgeSeconds: MaxAgeSeconds,
		MaxSizeBytes:  MaxSizeBytes,
		previousFiles: make([]string, 0),
		ticker:        time.NewTicker(time.Duration(MaxAgeSeconds) * time.Second),
		tickerDone:    make(chan bool),
	}

	// Kickstart the writer by creating a new file
	fw.Rotate()

	// Kickstart automatic file rotation on timer
	go fw.rotateOnTimer()

	return fw
}

// TODO: only upload based on timer if there is actually data

func (f *FileWriter) rotateOnTimer() {
	for {
		select {
		case <-f.tickerDone:
			log.Println("Stopping ticker for", f.DataDirectory)
			return
		case <-f.ticker.C:
			log.Println("Trying periodic rotate...")

			// rotating := f.rotating.TryLock()
			// if !rotating {
			// log.Println("Someone else is currently rotating, skipping this rotation")
			// } else {
			f.canWrite.Lock()
			fileinfo, err := f.fd.Stat()
			if err != nil {
				log.Println("Unable to auto rotate", err)
			}
			if fileinfo.Size() > 0 {
				f.Rotate()
			}
			f.canWrite.Unlock()
			// f.rotating.Unlock()
			// }
		}
	}
}

func (f *FileWriter) Rotate() error {
	// BLOCKS ALL WRITES while we rotate.
	// Could we be more clever here by opening the new file
	// in a new goroutine to continue write while we close the previous

	// Make sure only one rotation is happening at a time, as we do them asynchronously

	rotating := f.rotating.TryLock()
	if !rotating {
		log.Println("Someone else is currently rotating, skipping this rotation")
		return nil
	}
	defer f.rotating.Unlock()

	log.Println("Rotating!")
	var err error

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

	return nil
}

func (f *FileWriter) Write(data string) error {
	var err error

	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	// time.Sleep(5 * time.Second)
	// check to see if we will hit our file size limit
	fileinfo, err := f.fd.Stat()
	if err != nil {
		log.Println(err)
		return err
	}
	if fileinfo.Size()+int64(len(data)) > f.MaxSizeBytes {
		err = f.Rotate()
	}
	if err != nil {
		log.Println(err)
		return err
	}

	// write data
	_, err = f.fd.WriteString(data + "\n")
	if err != nil {
		log.Println(err)
	}

	return err
}

func (f *FileWriter) Close() error {
	f.ticker.Stop()
	f.tickerDone <- true

	// Close open file
	f.canWrite.Lock()
	err := f.Rotate()
	f.canWrite.Unlock()

	// Check on this error
	if err != nil {
		log.Println(err)
	}

	log.Println("Finishing uploading files")
	log.Println(f.previousFiles)

	return nil
}
