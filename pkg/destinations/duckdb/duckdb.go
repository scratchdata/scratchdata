package duckdb

import (
	"fmt"
	"io"
	"strconv"

	"github.com/rs/zerolog/log"
)

type DuckDBServer struct {
	Token string `mapstructure:"token"`
}

func (s *DuckDBServer) InsertBatchFromNDJson(input io.Reader) error {
	data, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	log.Debug().Bytes("data", data).Msg("Writing Data to dummy DB")

	return nil
}

func (s *DuckDBServer) QueryJSON(query string, writer io.Writer) error {
	log.Debug().Str("query", query).Msg("Querying")
	for i := 0; i < 10; i++ {
		data := "Data " + strconv.Itoa(i) + "\n"
		_, err := writer.Write([]byte(data))
		if err != nil {
			fmt.Println("Error writing data:", err)
			break
		}
	}

	return nil
}
