package redshift

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// QueryJSONStream executes a query on RedShift and streams results in JSON format to the provided writer.
func (r *RedshiftServer) QueryJSON(query string, writer io.Writer) error {
	rows, err := r.query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	// Prepare a buffer for scanning values into
	values := make([]sql.RawBytes, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// Write the opening bracket of the JSON array
	if _, err := writer.Write([]byte{'['}); err != nil {
		return err
	}

	// Iterate over rows and encode each one into JSON
	isFirstRow := true
	for rows.Next() {
		if !isFirstRow {
			// Write comma separator before each row, except the first one
			if _, err := writer.Write([]byte{','}); err != nil {
				return err
			}
		} else {
			isFirstRow = false
		}

		// Scan values into RawBytes buffer
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// Construct a map representing the row
		row := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			if values[i] != nil {
				v = string(values[i])
			} else {
				v = fmt.Sprintf("%v", values[i])
			}
			row[col] = v
		}

		// Encode the row to JSON and write it directly to the output stream
		if err := json.NewEncoder(writer).Encode(row); err != nil {
			return err
		}
	}

	// Write the closing bracket of the JSON array
	if _, err := writer.Write([]byte{']'}); err != nil {
		return err
	}

	return nil
}

// QueryCSV executes a query on RedShift and writes results in CSV format to the provided writer.
func (r *RedshiftServer) QueryCSV(query string, writer io.Writer) error {
	rows, err := r.query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(writer)
	defer csvWriter.Flush()

	// Write header
	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// Convert byte slices to strings
		stringValues := make([]string, len(values))
		for i, val := range values {
			if b, ok := val.([]byte); ok {
				stringValues[i] = string(b)
			} else {
				stringValues[i] = fmt.Sprintf("%v", val)
			}
		}

		// Write row
		if err := csvWriter.Write(stringValues); err != nil {
			return err
		}
	}

	return nil
}

// query executes the given query and returns the result rows.
func (r *RedshiftServer) query(query string) (*sql.Rows, error) {
	return r.conn.Query(query)
}
