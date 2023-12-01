package dummy

import (
	"fmt"
	"io"
	"scratchdb/config"
	"strconv"

	"github.com/rs/zerolog/log"
)

type DummyDBServer struct {
	config config.DummyConfig
}

func NewDummyDBServer(config config.DummyConfig) *DummyDBServer {
	return &DummyDBServer{config: config}
}

func (s *DummyDBServer) InsertBatchFromNDJson(input io.Reader) error {
	data, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	log.Debug().Bytes("data", data).Msg("Writing Data to dummy DB")

	return nil
}

func (s *DummyDBServer) QueryJSON(query string, writer io.Writer) error {
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
