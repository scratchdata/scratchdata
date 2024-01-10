package clickhouse

import (
	"bufio"
	"io"
	"scratchdata/models/postgrest"
	"scratchdata/util"

	"github.com/rs/zerolog/log"
)

func (s *ClickhouseServer) queryJSON(sql string, writer io.Writer) error {
	resp, err := s.httpQuery(sql)
	if err != nil {
		return err
	}
	defer resp.Close()

	writer.Write([]byte("["))

	// Treat the output as a linked list of text fragments.
	// Each fragment could be a partial JSON line
	var nextIsPrefix = true
	var nextErr error = nil
	var nextLine []byte
	reader := bufio.NewReader(resp)
	line, isPrefix, err := reader.ReadLine()

	for {
		// If we're at the end of our input, break
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Output the data
		writer.Write(line)

		// Check to see whether we are at the last row by looking for EOF
		nextLine, nextIsPrefix, nextErr = reader.ReadLine()

		// If the next row is not an EOF, then output a comma. This is to avoid a
		// trailing comma in our JSON
		if !isPrefix && nextErr != io.EOF {
			writer.Write([]byte(","))
		}

		// Equivalent of "currentPointer = currentPointer.next"
		line, isPrefix, err = nextLine, nextIsPrefix, nextErr
	}
	writer.Write([]byte("]"))

	return nil
}

func (s *ClickhouseServer) QueryJSON(query string, writer io.Writer) error {
	sanitized := util.TrimQuery(query)
	return s.queryJSON("SELECT * FROM ("+sanitized+") FORMAT "+"JSONEachRow", writer)
}

func (c *ClickhouseServer) QueryPostgrest(query postgrest.Postgrest, w io.Writer) error {
	b := &util.StringBuffer{}
	b.Printf(`SELECT * FROM (`)
	err := postgrest.AppendSQL(b, query)
	b.Printf(`) FORMAT JSONEachRow`)
	log.Debug().
		Err(err).
		Any("query", query).
		Str("sql", b.String()).
		Msg("ClickhouseServer.QueryPostgrest")
	if err != nil {
		return err
	}
	return c.queryJSON(b.String(), w)
}
