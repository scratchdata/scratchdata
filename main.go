package main

import (
	"github.com/scratchdata/scratchdata/cmd/scratchdata"
	"github.com/scratchdata/scratchdata/config"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Set default log format before we read config
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()

	var configOptions config.ScratchDataConfig

	defaultConfig := len(os.Args) == 1

	if defaultConfig {
		configOptions.API.Enabled = true
		configOptions.API.Port = 8080
		configOptions.API.MaxAgeSeconds = 1
		configOptions.API.MaxSizeBytes = 1000
		configOptions.Workers.Enabled = true
		configOptions.Workers.Count = 1
		configOptions.Database.Type = "static"
		configOptions.Cache.Type = "memory"
		configOptions.BlobStore.Type = "memory"
		configOptions.Queue.Type = "memory"
		configOptions.DataSink.Type = "memory"

		destination := config.Destination{
			Type: "duckdb",
			Settings: map[string]any{
				"file": "./data.duckdb",
			},
			APIKeys: []string{"local"},
		}
		configOptions.Destinations = append(configOptions.Destinations, destination)

		log.Info().Msg("No config file specified, using local default values")
	} else {
		err := cleanenv.ReadConfig(os.Args[1], &configOptions)
		if err != nil {
			log.Fatal().Err(err).Msg("Unable to read configuration file")
		}
	}

	storageServices, err := scratchdata.GetStorageServices(configOptions)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to initialize storage")
	}
	scratchdata.Run(configOptions, storageServices)
}
