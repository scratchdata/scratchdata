package redshift

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

func (s *RedshiftServer) QueryNDJson(query string, writer io.Writer, params map[string]any) error {
	return errors.New("not implemented")
}

func (s *RedshiftServer) QueryJSON(query string, writer io.Writer, params map[string]any) error {
	rows, err := s.conn.Query(query)
	if err != nil {
		log.Error().Err(err).Msg("failed to execute query")
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Error().Err(err).Msg("failed to get column names")
		return err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	_, err = writer.Write([]byte("["))
	if err != nil {
		log.Error().Err(err).Msg("failed to write JSON array start:")
		return err
	}

	firstRow := true
	encoder := json.NewEncoder(writer)
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Error().Err(err).Msg("failed to scan row values")
			return err
		}

		jsonObject := make(map[string]interface{})
		for i, column := range columns {
			jsonObject[column] = values[i]
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

		err = encoder.Encode(jsonObject)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal JSON")
			return err
		}

	}

	_, err = writer.Write([]byte("]"))
	if err != nil {
		log.Error().Err(err).Msg("failed to write JSON array end")
		return err
	}

	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("failed to iterate over all rows")
		return err
	}

	return nil
}

func (s *RedshiftServer) QueryCSV(query string, writer io.Writer, params map[string]any) error {
	rows, err := s.conn.Query(query)
	if err != nil {
		log.Error().Err(err).Msg("failed to execute query")
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Error().Err(err).Msg("failed to get column names")
		return err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}
	encoder := csv.NewWriter(writer)
	// Write column names to the writer
	err = encoder.Write(columns)
	if err != nil {
		log.Error().Err(err).Msg("failed to write column names")
		return err
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			log.Error().Err(err).Msg("failed to scan row values")
			return err
		}

		csvRow := make([]string, len(columns))
		for i, value := range values {
			if value == nil {
				csvRow[i] = "null"
			} else {
				csvRow[i] = fmt.Sprintf("%v", value)
			}
		}

		err = encoder.Write(csvRow)
		if err != nil {
			log.Error().Err(err).Msg("failed to write CSV row")
			return err
		}
	}

	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("failed to iterate rows")
		return err
	}

	encoder.Flush()

	return nil
}
