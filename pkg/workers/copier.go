package workers

import (
	"bufio"
	"context"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/api"
	"github.com/scratchdata/scratchdata/pkg/util"
)

var copyDir string = "copy"

func (w *ScratchDataWorker) CopyData(sourceId int64, query string, destId uint, destTable string) error {
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
	defer os.RemoveAll(localFolder)

	source, err := w.destinationManager.Destination(ctx, sourceId)
	if err != nil {
		return err
	}

	dest, err := w.destinationManager.Destination(ctx, int64(destId))
	if err != nil {
		return err
	}

	writer := util.NewChunkedWriter(w.Config.MaxBulkQuerySizeBytes, w.Config.BulkChunkSizeBytes, localFolder)
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

	// Algorithm demuxes NDJSON that is nested into JSON with multiple tables

	flattener := api.NewMultiTableFlattener()
	fds := map[string]*util.ChunkedWriter{}
	maxCapacity := 1024 * 1024 * 10
	buf := make([]byte, maxCapacity)

	for _, f := range files {
		path := filepath.Join(localFolder, f.Name())
		fd, err := os.Open(path)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(fd)
		scanner.Buffer(buf, maxCapacity)

		for scanner.Scan() {
			line := scanner.Text()
			flatItems, err := flattener.Flatten(destTable, line)
			if err != nil {
				return err
			}

			for _, item := range flatItems {

				tableFd, ok := fds[item.Table]
				if !ok {
					ndjsonPath := filepath.Join(localFolder, "tables", item.Table)
					err = os.MkdirAll(ndjsonPath, os.ModePerm)
					if err != nil {
						return err
					}

					tableFd = util.NewChunkedWriter(w.Config.MaxBulkQuerySizeBytes, w.Config.BulkChunkSizeBytes, ndjsonPath)
					fds[item.Table] = tableFd
				}

				tableFd.Write([]byte(item.JSON))
				tableFd.Write([]byte("\n"))
			}
		}

		fd.Close()
	}

	for table, fd := range fds {
		err = fd.Close()
		if err != nil {
			return err
		}

		folderName := filepath.Join(localFolder, "tables", table)
		files, err := os.ReadDir(folderName)
		if err != nil {
			return err
		}

		err = dest.CreateEmptyTable(destTable)
		if err != nil {
			return err
		}

		for _, f := range files {
			path := filepath.Join(folderName, f.Name())
			err = dest.CreateColumns(table, path)
			if err != nil {
				log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", table).Msg("Unable to create columns")
				continue
			}
			err = dest.InsertFromNDJsonFile(table, path)
			if err != nil {
				log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", table).Msg("Unable to insert data")
				continue
			}
		}

	}

	// This is the regular algorithm without demuxing 1 table to many. Assumes the NDJSON is flat.
	// err = dest.CreateEmptyTable(destTable)
	// if err != nil {
	// 	return err
	// }

	// for _, f := range files {
	// 	path := filepath.Join(localFolder, f.Name())
	// 	err = dest.CreateColumns(destTable, path)
	// 	if err != nil {
	// 		log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", destTable).Msg("Unable to create columns")
	// 		continue
	// 	}
	// 	err = dest.InsertFromNDJsonFile(destTable, path)
	// 	if err != nil {
	// 		log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", destTable).Msg("Unable to insert data")
	// 		continue
	// 	}
	// }

	return nil
}
