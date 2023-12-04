package dummy

import (
	"fmt"
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

	for i := 0; i < 10; i++ {
		var err error

		// Demonstrates how to use configuration values
		if s.UseAllCaps {
			_, err = fmt.Fprintf(writer, "DATA %d\n", i)
		} else {
			_, err = fmt.Fprintf(writer, "data %d\n", i)
		}

		if err != nil {
			log.Error().Err(err).Msg("Error writing data:")
			break
		}
	}

	return nil
}
