package bigquery

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
)

func (b *BigQueryServer) QueryJSON(query string, writer io.Writer) error {
	// NOTE: Query should be with dataset as prefix. Example: SELECT * FROM `dataset.table`

	ctx := context.TODO()
	q := b.conn.Query(query)

	writer.Write([]byte("["))
	enc := json.NewEncoder(writer)

	itr, err := q.Read(ctx)
	if err != nil {
		log.Error().Err(err).Msg("error getting query iterator")
		return err
	}
	firstRow := true
	for {
		var data map[string]bigquery.Value
		err := itr.Next(&data)

		if err == iterator.Done {
			break
		}
		if !firstRow {
			_, err = writer.Write([]byte(","))
			if err != nil {
				log.Error().Err(err).Msg("failed to write JSON array separator")
				return err
			}
		} else {
			firstRow = false
		}

		err = enc.Encode(data)
		if err != nil {
			return err
		}
	}
	writer.Write([]byte("]"))
	return nil
}

func (b *BigQueryServer) QueryCSV(query string, writer io.Writer) error {
	// NOTE: Query should be with dataset as prefix. Example: SELECT * FROM `dataset.table`

	ctx := context.TODO()
	q := b.conn.Query(query)

	enc := csv.NewWriter(writer)

	columnsQuery := b.conn.Query(query)
	columnsItr, err := columnsQuery.Read(ctx)
	if err != nil {
		log.Error().Err(err).Msg("error getting columns query iterator")
		return err
	}

	columns := make([]string, 0)
	columnsRow := make(map[string]bigquery.Value)
	if err := columnsItr.Next(&columnsRow); err != nil {
		log.Error().Err(err).Msg("error retrieving columns")
		return err
	}
	for columnName := range columnsRow {
		columns = append(columns, columnName)
	}

	if err := enc.Write(columns); err != nil {
		log.Error().Err(err).Msg("error writing columns to CSV")
		return err
	}

	dataItr, err := q.Read(ctx)
	if err != nil {
		log.Error().Err(err).Msg("error getting data query iterator")
		return err
	}

	for {
		var dataRow map[string]bigquery.Value
		err := dataItr.Next(&dataRow)

		if err == iterator.Done {
			break
		}

		row := make([]string, len(columns))
		for i, columnName := range columns {
			val := dataRow[columnName]
			if val != nil {
				// not sure what happens here as soons as I use val, column format changes, so using at it is
				row[i] = fmt.Sprintf("%v", dataRow[columnName])
			} else {
				row[i] = "null"
			}

		}

		if err := enc.Write(row); err != nil {
			log.Error().Err(err).Msg("error writing data row to CSV")
			return err
		}
	}

	enc.Flush()
	if err := enc.Error(); err != nil {
		log.Error().Err(err).Msg("error flushing CSV writer")
		return err
	}

	return nil
}
