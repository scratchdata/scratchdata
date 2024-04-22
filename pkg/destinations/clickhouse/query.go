package clickhouse

import (
	"bufio"
	"io"

	"github.com/scratchdata/scratchdata/pkg/util"
)

func (s *ClickhouseServer) QueryNDJson(query string, writer io.Writer, params map[string]any) error {
	sanitized := util.TrimQuery(query)
	sql := "SELECT * FROM (" + sanitized + ") FORMAT " + "JSONEachRow"

	resp, err := s.httpQuery(sql)
	if err != nil {
		return err
	}
	defer resp.Close()

	_, err = io.Copy(writer, resp)
	return err
}

func (s *ClickhouseServer) QueryJSON(query string, writer io.Writer, params map[string]any) error {
	sanitized := util.TrimQuery(query)
	sql := "SELECT * FROM (" + sanitized + ") FORMAT " + "JSONEachRow"

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

func (s *ClickhouseServer) QueryCSV(query string, writer io.Writer, params map[string]any) error {
	sanitized := util.TrimQuery(query)
	sql := "SELECT * FROM (" + sanitized + ") FORMAT " + "CSVWithNames"

	resp, err := s.httpQuery(sql)
	if err != nil {
		return err
	}
	defer resp.Close()

	_, err = io.Copy(writer, resp)

	return err
}
