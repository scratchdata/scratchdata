package redshift

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func (s *RedshiftServer) QueryJSON(query string, writer io.Writer) error {

	rows, err := s.conn.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	
	jsonObjects := []map[string]interface{}{}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}

		// Populate the JSON object with column values
		jsonObject := make(map[string]interface{})
		for i, column := range columns {
			jsonObject[column] = values[i]
		}

		jsonObjects = append(jsonObjects, jsonObject)
	}

	
	jsonData, err := json.Marshal(jsonObjects)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %v", err)
	}

	
	_, err = writer.Write(jsonData)
	if err != nil {
		return fmt.Errorf("failed to write JSON: %v", err)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate over rows: %v", err)
	}

	return nil
}

func (s *RedshiftServer) QueryCSV(query string, writer io.Writer) error {
	rows, err := s.conn.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get column names: %v", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	
	_, err = writer.Write([]byte(strings.Join(columns, ",") + "\n"))
	if err != nil {
		return fmt.Errorf("failed to write column names: %v", err)
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row values: %v", err)
		}

		// Write row values as CSV objects to the writer
		csvRow := make([]string, len(columns))
		for i, value := range values {
			csvRow[i] = fmt.Sprintf("%v", value)
		}
		_, err = writer.Write([]byte(strings.Join(csvRow, ",") + "\n"))
		if err != nil {
			return fmt.Errorf("failed to write CSV row: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate over rows: %v", err)
	}

	return nil
}
