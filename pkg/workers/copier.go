package workers

import (
	"context"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
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

	err = dest.CreateEmptyTable(destTable)
	if err != nil {
		return err
	}

	for _, f := range files {
		path := filepath.Join(localFolder, f.Name())
		err = dest.CreateColumns(destTable, path)
		if err != nil {
			log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", destTable).Msg("Unable to create columns")
			continue
		}
		err = dest.InsertFromNDJsonFile(destTable, path)
		if err != nil {
			log.Error().Err(err).Int64("source_id", sourceId).Uint("dest_id", destId).Str("table", destTable).Msg("Unable to insert data")
			continue
		}
	}

	return nil
}
