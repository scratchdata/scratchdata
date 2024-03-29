package workers

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	fileIndex   int
	currentFile *os.File

	mutex sync.Mutex
}

func NewSplitWriter(maxSize int, chunkSize int, folder string) *SplitWriter {
	return &SplitWriter{
		maxSize:   maxSize,
		chunkSize: chunkSize,
		folder:    folder,
	}
}

func (sw *SplitWriter) Write(p []byte) (n int, err error) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	if sw.currentSize+len(p) > sw.maxSize {
		return 0, errors.New("exceeded maximum size")
	}

	scanner := bufio.NewScanner(bytes.NewReader(p))
	for scanner.Scan() {
		line := scanner.Bytes()
		if sw.currentFile == nil || sw.currentSize+len(line) > sw.chunkSize {
			if sw.currentFile != nil {
				sw.currentFile.Close()
			}
			sw.fileIndex++
			filePath := filepath.Join(sw.folder, fmt.Sprintf("file%d.txt", sw.fileIndex))
			sw.currentFile, err = os.Create(filePath)
			if err != nil {
				return 0, err
			}
			sw.currentSize = 0
		}
		n, err = sw.currentFile.Write(line)
		sw.currentSize += n
		if err != nil {
			return n, err
		}
	}
	return len(p), nil
}

func (sw *SplitWriter) Writex(p []byte) (n int, err error) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	// Check to see if we're going to go over max file size
	if sw.currentSize+len(p) > sw.maxSize {
		return 0, errors.New("max file size exceeded")
	}

	// If there's no open file, create it
	if sw.currentFile == nil {
		filePath := filepath.Join(sw.folder, fmt.Sprintf("file-%d", sw.fileIndex))
		sw.currentFile, err = os.Create(filePath)
		if err != nil {
			return 0, err
		}
	}

	return 0, nil
}

func (sw *SplitWriter) Close() error {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	if sw.currentFile != nil {
		return sw.currentFile.Close()
	}
	return nil
}

func (w *ScratchDataWorker) CopyData(sourceId int64, query string, destId int64, destTable string) error {
	ctx := context.TODO()

	snowflake, err := util.NewSnowflakeGenerator()
	if err != nil {
		return err
	}

	localFolder := filepath.Join(w.Config.DataDirectory, copyDir, snowflake.Generate().String())
	err = os.MkdirAll(localFolder, os.ModePerm)
	if err != nil {
		return err
	}
	// defer os.RemoveAll(localFolder)

	source, err := w.destinationManager.Destination(ctx, sourceId)
	if err != nil {
		return err
	}

	dest, err := w.destinationManager.Destination(ctx, destId)
	if err != nil {
		return err
	}

	// writer := NewSplitWriter(w.Config.MaxBulkQuerySizeBytes, w.Config.BulkChunkSizeBytes, localFolder)
	writer, err := os.Create(filepath.Join(localFolder, "data.ndjson"))
	if err != nil {
		return err
	}

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

	err = dest.CreateEmptyTable(destTable)
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
