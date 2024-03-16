package redshift

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// QueryJSON executes a query on RedShift and writes results in JSON format to the provided writer.
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

	result := make([]map[string]interface{}, 0)
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			row[col] = v
		}
		result = append(result, row)
	}

	return json.NewEncoder(writer).Encode(result)
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
