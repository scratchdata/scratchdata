package duckdb

import (
	"io"
	"os"
	"path/filepath"
	"scratchdata/util"
	"syscall"

	"github.com/rs/zerolog/log"
)

func (s *DuckDBServer) QueryJSON(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

	pipeFile := "query.pipe"

	dirName, err := os.MkdirTemp("", "query")
	if err != nil {
		return err
	}
	defer os.Remove(dirName)

	pipePath := filepath.Join(dirName, pipeFile)

	err = syscall.Mkfifo(pipePath, 0666)
	if err != nil {
		return err
	}
	defer os.Remove(pipePath)

	go func() {
		res, err := s.db.Exec("COPY ("+sanitized+") TO ? (FORMAT JSON, ARRAY true) ", pipePath)
		// res, err := s.db.Exec("COPY (" + sanitized + ") TO 'p.pipe' (FORMAT JSON, ARRAY true) ")
		log.Print(err)
		log.Print(res.LastInsertId())
		log.Print(res.RowsAffected())
	}()

	pipe, err := os.OpenFile("p.pipe", os.O_CREATE|os.O_RDONLY, os.ModeNamedPipe)
	log.Print(io.Copy(writer, pipe))
	return nil

	rows, err := s.db.Query("DESCRIBE " + sanitized)
	if err != nil {
		return err
	}

	var columnName string
	var columnType *string
	var null *string
	var key *string
	var defaultVal *interface{}
	var extra *string
	columnNames := make([]string, 0)

	for rows.Next() {
		err := rows.Scan(&columnName, &columnType, &null, &key, &defaultVal, &extra)
		if err != nil {
			return err
		}
		columnNames = append(columnNames, columnName)
	}

	rows.Close()

	rows, err = s.db.Query("SELECT to_json(COLUMNS(*)) FROM (" + sanitized + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	writer.Write([]byte("["))

	// https://groups.google.com/g/golang-nuts/c/-9h9UwrsX7Q
	pointers := make([]interface{}, len(cols))
	container := make([]*string, len(cols))

	for i, _ := range pointers {
		pointers[i] = &container[i]
	}

	hasNext := rows.Next()
	for hasNext {
		err := rows.Scan(pointers...)
		if err != nil {
			return err
		}

		writer.Write([]byte("{"))
		for i, _ := range cols {
			writer.Write([]byte("\""))
			writer.Write([]byte(util.JsonEscape(columnNames[i])))
			writer.Write([]byte("\""))

			writer.Write([]byte(":"))

			if container[i] == nil {
				writer.Write([]byte("null"))
			} else {
				writer.Write([]byte(*container[i]))
			}

			if i < len(cols)-1 {
				writer.Write([]byte(","))
			}
		}

		writer.Write([]byte("}"))

		hasNext = rows.Next()

		if hasNext {
			writer.Write([]byte(","))
		}
	}

	writer.Write([]byte("]"))

	return nil
}
