package duckdb

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"
)

func (s *DuckDBServer) QueryPipe(query string, format string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

	dir, err := os.MkdirTemp("", "scratchdata_duckdb")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	fifoPath := filepath.Join(dir, "p.pipe")
	log.Trace().Str("pipe", fifoPath).Msg("DuckDB pipe")
	err = syscall.Mkfifo(fifoPath, 0666)
	if err != nil {
		return err
	}

	pipeFd, err := syscall.Open(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return err
	}
	defer syscall.Close(pipeFd)

	var formatClause string
	switch format {
	case "csv":
		formatClause = "(FORMAT CSV)"
	default:
		formatClause = "(FORMAT JSON, ARRAY TRUE)"
	}
	sql := "COPY (" + sanitized + ") TO '" + fifoPath + "' " + formatClause
	// log.Trace().Str(sql, sql).Send()

	errExecChan := make(chan error)
	go func() {
		_, err := s.db.Exec(sql)
		log.Error().Err(err).Send()
		errExecChan <- err
	}()

	readyToStop := false
	buf := make([]byte, 1024)
	for {
		n, _ := syscall.Read(pipeFd, buf)

		if n > 0 {
			writer.Write(buf[:n])
		} else {
			if readyToStop {
				break
			}

			select {
			case e := <-errExecChan:
				if e != nil {
					return e
				} else {
					readyToStop = true
				}
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	return nil
}

func (s *DuckDBServer) QueryJSONString(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)

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

func (s *DuckDBServer) QueryJSON(query string, writer io.Writer) error {
	return s.QueryPipe(query, "json", writer)
	// return s.QueryJSONString(query, writer)
}

func (s *DuckDBServer) QueryCSV(query string, writer io.Writer) error {
	return s.QueryPipe(query, "csv", writer)
}
