package clickhouse

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"scratchdata/util"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/rs/zerolog/log"
)

type ClickhouseServer struct {
	HTTPProtocol string `mapstructure:"protocol"`
	Host         string `mapstructure:"host"`
	HTTPPort     int    `mapstructure:"http_port"`
	TCPPort      int    `mapstructure:"tcp_port"`
	Username     string `mapstructure:"username"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`

	StoragePolicy string `mapstructure:"storage_policy"`

	MaxOpenConns        int  `mapstructure:"max_open_conns"`
	MaxIdleConns        int  `mapstructure:"max_idle_conns"`
	ConnMaxLifetimeSecs int  `mapstructure:"conn_max_lifetime_secs"`
	TLS                 bool `mapstructure:"tls"`
}

func (s *ClickhouseServer) InsertBatchFromNDJson(input io.ReadSeeker) error {
	return errors.New("Not implemented for clickhouse")
}

func (s *ClickhouseServer) httpQuery(query string, clickhouseFormat string) (io.ReadCloser, error) {
	sanitized := util.TrimQuery(query)
	sql := "SELECT * FROM (" + sanitized + ") FORMAT " + clickhouseFormat

	url := fmt.Sprintf("%s://%s:%d", s.HTTPProtocol, s.Host, s.HTTPPort)

	var jsonStr = []byte(sql)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Clickhouse-User", s.Username)
	req.Header.Set("X-Clickhouse-Key", s.Password)
	req.Header.Set("X-Clickhouse-Database", s.Database)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error().Err(err).Msg("request failed")
		return nil, err
	}

	return resp.Body, nil
}

func (s *ClickhouseServer) QueryJSON(query string, writer io.Writer) error {
	resp, err := s.httpQuery(query, "JSONEachRow")
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
