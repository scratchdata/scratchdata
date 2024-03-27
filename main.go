package main

import (
	"embed"
	"fmt"
	"os"

	"github.com/scratchdata/scratchdata/cmd/scratchdata"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/datasink"
	"github.com/scratchdata/scratchdata/pkg/destinations"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

//go:embed config.yaml
var defaultConfig embed.FS

func main() {
	// Set default log format before we read config
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()

	var configOptions config.ScratchDataConfig

	useDefaultConfig := len(os.Args) == 1

	if useDefaultConfig {
		log.Info().Msg("No config file specified, using local default values")

		config, err := defaultConfig.ReadFile("config.yaml")
		if err != nil {
			log.Fatal().Err(err).Msg("Unable to read config")
		}
		fmt.Println(string(config))

		f, err := defaultConfig.Open("config.yaml")
		if err != nil {
			log.Fatal().Err(err).Msg("Unable to parse open config")
		}
		err = cleanenv.ParseYAML(f, &configOptions)
		if err != nil {
			log.Fatal().Err(err).Msg("Unable to read config")
		}

		f.Close()
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

	destinationManager := destinations.NewDestinationManager(storageServices)

	dataSink, err := datasink.NewDataSink(configOptions.DataSink, storageServices)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to set up data sink")
	}

	mux, err := scratchdata.GetMux(storageServices, destinationManager, dataSink, configOptions.API)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to set up data sink")
	}

	scratchdata.Run(configOptions, storageServices, destinationManager, dataSink, mux)
}
