package duckdb

import (
	"github.com/scratchdata/scratchdata/util"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"
)

func (s *DuckDBServer) QueryPipe(query string, format string, writer io.Writer) error {
	// This function is complicated. It does the following:
	//
	// 1. Creates a named pipe (mkfifo)
	// 2. Instructs DuckDB to execute the query and output to that pipe
	// 3. Copies data from the pipe to "writer" for the end user
	//
	// It is complicated because it uses nonblocking IO to both look for data from the pipe
	// and look for errors.

	sanitized := util.TrimQuery(query)

	// Create named pipe for our query
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

	// Open the named pipe locally for reading using non-blocking io (O_NONBLOCK)
	pipeFd, err := syscall.Open(fifoPath, os.O_RDONLY|syscall.O_NONBLOCK, 0644)
	if err != nil {
		return err
	}
	defer syscall.Close(pipeFd)

	// Generate the SQL query taking the format (csv, json) into account
	// Writes result to our pipe
	var formatClause string
	switch format {
	case "csv":
		formatClause = "(FORMAT CSV)"
	default:
		formatClause = "(FORMAT JSON, ARRAY TRUE)"
	}
	sql := "COPY (" + sanitized + ") TO '" + fifoPath + "' " + formatClause

	log.Trace().Str(sql, sql).Send()

	// Execute the query in a new goroutine. This will block while waiting
	// for someone to consume the pipe
	errExecChan := make(chan error)
	go func() {
		_, err := s.db.Exec(sql)
		if err != nil {
			log.Error().Err(err).Send()
		}
		errExecChan <- err
	}()

	// Indicates the sql query is finished, and we just need to read the rest of the buffer
	readyToStop := false

	// 1k buffer for data
	buf := make([]byte, 1024)

	// Loop until query is done. We know it is done because we will
	// receive on the errExecChan
	for {
		// Attempt to read data from pipe
		n, _ := syscall.Read(pipeFd, buf)

		if n > 0 {
			// If we have data, write it to the http handler and
			// keep checking for more data
			writer.Write(buf[:n])
		} else {
			// Otherwise, check and see if we have 0 data and the query is done
			// executing. If so, then we assume we've consumed all data and can return
			if readyToStop {
				break
			}

			// Has the SQL query finished executing?
			select {
			case e := <-errExecChan:
				if e != nil {
					// The query has finished executing and there was an error. Return.
					return e
				} else {
					// The query has finished and now we just need to consume the remainder of the pipe.
					// readyToStop = true means "keep consuming data and stop checking for more if read returns 0 bytes"
					readyToStop = true
				}
			default:
				// Query has not finished executing. Wait for a little bit and check again.
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
