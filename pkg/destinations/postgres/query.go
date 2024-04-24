package postgres

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

func (s *PostgresServer) QueryNDJson(query string, writer io.Writer) error {
	r, w := io.Pipe()
	// errChan := make(chan error)
	go func() {
		queryErr := s.QueryJSON(query, w)
		if queryErr != nil {
			w.CloseWithError(queryErr)
		} else {
			w.Close()
		}
	}()

	dec := json.NewDecoder(r)

	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return err
	}

	// TODO: stream all of this instead of decoding
	// while the array contains values
	for dec.More() {
		var m map[string]any
		err := dec.Decode(&m)
		if err != nil {
			return err
			// log.Fatal(err)
		}

		j, e := json.Marshal(m)
		if e != nil {
			return e
		}

		_, err = writer.Write(j)
		if err != nil {
			return err
		}

		_, err = writer.Write([]byte{'\n'})
		if err != nil {
			return err
		}
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return err
	}

	return nil
}

func (s *PostgresServer) QueryJSON(query string, writer io.Writer) error {
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

func (s *PostgresServer) QueryCSV(query string, writer io.Writer) error {
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
