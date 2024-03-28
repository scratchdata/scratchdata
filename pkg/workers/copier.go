package workers

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/util"
)

var copyDir string = "copy"

type SplitWriter struct {
	maxSize   int
	chunkSize int
	folder    string

	currentSize int
	currentFile *os.File

	mutex sync.Mutex
}

func (sw *SplitWriter) Write(p []byte) (n int, err error) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	for len(p) > 0 {
		if sw.currentFile == nil || sw.currentSize+len(p) > sw.maxSize {
			if sw.currentFile != nil {
				sw.currentFile.Close()
			}
			sw.currentSize = 0
			sw.currentFile, err = sw.createNextFile()
			if err != nil {
				return n, err
			}
		}

		writeSize := len(p)
		if sw.currentSize+(writeSize) > sw.maxSize {
			// Find the last newline character within the chunk size limit
			lastNewline := bytes.LastIndexByte(p[:sw.maxSize-sw.currentSize], '\n')
			if lastNewline != -1 {
				writeSize = lastNewline + 1 // Include the newline character in the chunk
			} else {
				// If no newline is found within the chunk size limit, write the entire chunk
				writeSize = sw.maxSize - sw.currentSize
			}
		}

		written, err := sw.currentFile.Write(p[:writeSize])
		n += written
		sw.currentSize += (written)
		p = p[writeSize:]

		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (sw *SplitWriter) Close() error {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	if sw.currentFile != nil {
		err := sw.currentFile.Close()
		sw.currentFile = nil
		return err
	}
	return nil
}

func (sw *SplitWriter) createNextFile() (*os.File, error) {
	fileName := filepath.Join(sw.folder, fmt.Sprintf("chunk_%d.dat", sw.currentSize))
	file, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	return file, nil
}

func NewSplitWriter(maxSize int, chunkSize int, folder string) *SplitWriter {
	return &SplitWriter{
		maxSize:   maxSize,
		chunkSize: chunkSize,
		folder:    folder,
	}
}

func (w *ScratchDataWorker) CopyData(sourceId int64, query string, destId int64, destTable string) error {
	ctx := context.TODO()

	snowflake, err := util.NewSnowflakeGenerator()
	if err != nil {
		return err
	}

	localFolder := filepath.Join(w.Config.DataDirectory, copyDir, snowflake.Generate().String())
	defer os.RemoveAll(localFolder)

	source, err := w.destinationManager.Destination(ctx, sourceId)
	if err != nil {
		return err
	}

	dest, err := w.destinationManager.Destination(ctx, destId)
	if err != nil {
		return err
	}

	err = dest.CreateEmptyTable(destTable)
	if err != nil {
		return err
	}

	writer := NewSplitWriter(w.Config.MaxBulkQuerySizeBytes, w.Config.BulkChunkSizeBytes, localFolder)

	err = source.QueryNDJson(query, writer)
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	files, err := os.ReadDir(localFolder)
	if err != nil {
		return err
	}

	for _, f := range files {
		path := filepath.Join(localFolder, f.Name())
		err = dest.CreateColumns(destTable, path)
		if err != nil {
			log.Error().Err(err).Int64("source_id", sourceId).Int64("dest_id", destId).Str("table", destTable).Msg("Unable to create columns")
			continue
		}
		err = dest.InsertFromNDJsonFile(destTable, path)
		if err != nil {
			log.Error().Err(err).Int64("source_id", sourceId).Int64("dest_id", destId).Str("table", destTable).Msg("Unable to insert data")
			continue
		}
	}

	return nil
}
