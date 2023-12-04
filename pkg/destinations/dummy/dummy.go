package dummy

import (
	"io"

	"github.com/rs/zerolog/log"
)

type DummyDBServer struct {
	UseAllCaps bool `mapstructure:"all_caps"`
}

func (s *DummyDBServer) InsertBatchFromNDJson(input io.ReadSeeker) error {
	data, err := io.ReadAll(input)
	if err != nil {
		return err
	}
	log.Debug().Bytes("data", data).Msg("Writing Data to dummy DB")

	return nil
}

func (s *DummyDBServer) QueryJSON(query string, writer io.Writer) error {
	log.Debug().Str("query", query).Msg("Querying")

	var output string

	if s.UseAllCaps {
		output = `[{"HELLO": "WORLD"}]`
	} else {
		output = `[{"hello": "world"}]`
	}
	i, err := writer.Write([]byte(output))

	log.Debug().Int("bytes_written", i).Send()
	return err
}
