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

	// Push closed files to a longer term storage
	pusherDone chan bool

	// Ensure only 1 rotation is happening at a time
	rotating sync.Mutex
	// Ensure only 1 file write (or rotate) is happening at a time
	canWrite sync.Mutex

	// Used to rotate every x interval
	ticker     *time.Ticker
	tickerDone chan bool

	wg sync.WaitGroup
}

func NewFileWriter(DataDirectory string, MaxAgeSeconds int, MaxSizeBytes int64) *FileWriter {
	fw := &FileWriter{
		DataDirectory: DataDirectory,
		MaxAgeSeconds: MaxAgeSeconds,
		MaxSizeBytes:  MaxSizeBytes,
		ticker:        time.NewTicker(time.Duration(MaxAgeSeconds) * time.Second),
		tickerDone:    make(chan bool),
		pusherDone:    make(chan bool),
	}

	// Kickstart the writer by creating a new file
	fw.Rotate(true)

	// Kickstart automatic file rotation on timer
	fw.wg.Add(1)
	go fw.rotateOnTimer()

	fw.wg.Add(1)
	go fw.pushFiles()

	return fw
}

func (f *FileWriter) rotateOnTimer() {
	defer f.wg.Done()

	for {
		select {
		case <-f.tickerDone:
			log.Println("Stopping ticker for", f.DataDirectory)
			return
		case <-f.ticker.C:
			// log.Println("Trying periodic rotate...")

			f.canWrite.Lock()
			fileinfo, err := f.fd.Stat()
			if err != nil {
				log.Println("Unable to auto rotate", err)
			}
			if fileinfo.Size() > 0 {
				f.Rotate(true)
			}
			f.canWrite.Unlock()
		}
	}
}

func (f *FileWriter) pushFiles() {
	defer f.wg.Done()

	keepReading := true
	for keepReading {
		select {
		case <-f.pusherDone:
			log.Println("Finishing uploading remaining files, then will stop")
			keepReading = false
		default:
		}

		// log.Println("Checking for files to upload")

		uploadPath := filepath.Join(f.DataDirectory, "closed")
		entries, err := os.ReadDir(uploadPath)
		if err != nil {
			log.Println(err)
			continue
		}

		for _, e := range entries {
			filename := filepath.Join(uploadPath, e.Name())
			fileinfo, err := e.Info()

			if err != nil {
				log.Println("Unable to get info for file", filename, err)
			}

			if fileinfo.Size() > 0 {
				log.Println("Uploading", e.Name(), "to s3")
			}

			err = os.Remove(filename)
			if err != nil {
				log.Println("Unable to remove file", filename, err)
			}
		}

		time.Sleep(1 * time.Second)
	}
}

func (f *FileWriter) Rotate(createNew bool) error {
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

	// log.Println("Rotating!")
	var err error

	// Check to see if we have an open fd
	if f.fd != nil {
		fileinfo, err := f.fd.Stat()
		if err != nil {
			log.Println(err)
			return err
		}

		oldName := f.fd.Name()
		filename := fileinfo.Name()

		err = f.fd.Close()
		if err != nil {
			log.Println(err)
			return err
		}

		newDir := filepath.Join(f.DataDirectory, "closed")
		err = os.MkdirAll(newDir, os.ModePerm)
		if err != nil {
			log.Println(err)
			return err
		}

		err = os.Rename(oldName, filepath.Join(newDir, filename))
		if err != nil {
			log.Println(err)
			return err
		}
	}

	if createNew {
		newFileId := ulid.Make().String()
		dir := filepath.Join(f.DataDirectory, "open")
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Println(err)
			return err
		}

		path := filepath.Join(dir, newFileId+".ndjson")

		f.fd, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println(err)
			return err
		}
	}

	return nil
}

func (f *FileWriter) Write(data string) error {
	var err error

	f.canWrite.Lock()
	defer f.canWrite.Unlock()

	// check to see if we will hit our file size limit
	fileinfo, err := f.fd.Stat()
	if err != nil {
		log.Println(err)
		return err
	}
	if fileinfo.Size()+int64(len(data)) > f.MaxSizeBytes {
		err = f.Rotate(true)
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
	err := f.Rotate(false)
	f.canWrite.Unlock()

	// Check on this error
	if err != nil {
		log.Println(err)
	}

	log.Println("Finishing uploading files")
	f.pusherDone <- true

	f.wg.Wait()

	return nil
}
