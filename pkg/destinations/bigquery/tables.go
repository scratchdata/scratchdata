package bigquery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/models"
	"google.golang.org/api/iterator"
)

func (b *BigQueryServer) Columns(table string) ([]models.Column, error) {
	rc := []models.Column{}

	tokens := strings.Split(table, ".")
	if len(tokens) != 2 {
		return nil, errors.New("Table should be in the format dataset.table")
	}

	datasetID := tokens[0]
	tableID := tokens[1]

	ctx := context.TODO()
	tableInfo := b.conn.Dataset(datasetID).Table(tableID)
	meta, err := tableInfo.Metadata(ctx)
	if err != nil {
		return nil, err
	}

	for _, field := range meta.Schema {
		rc = append(rc, models.Column{
			Name: field.Name,
			Type: string(field.Type),
		})
	}
	return rc, nil
}

func (b *BigQueryServer) Tables() ([]string, error) {
	rc := []string{}

	ctx := context.TODO()
	it := b.conn.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		// For each dataset, list all tables.
		tableIt := dataset.Tables(ctx)
		for {
			table, err := tableIt.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Error().Err(err).Str("dataset_id", dataset.DatasetID).Msg("Failed to list tables")
				continue
			}
			rc = append(rc, fmt.Sprintf("%s.%s", dataset.DatasetID, table.TableID))
		}
	}

	return rc, nil
}
