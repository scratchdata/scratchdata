package main

import (
	"context"
	"os"
	"os/signal"
	"scratchdata/cmd"
	"scratchdata/cmd/api"
	"scratchdata/config"
	"strconv"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func setupLogs(logConfig config.Logs) {
	// Equivalent of Lshortfile
	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		short := file
		for i := len(file) - 1; i > 0; i-- {
			if file[i] == '/' {
				short = file[i+1:]
				break
			}
		}
		file = short
		return file + ":" + strconv.Itoa(line)
	}

	// Set log level
	zerolog.SetGlobalLevel(logConfig.ToLevel())

	// Set log output format
	if logConfig.Pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
	} else {
		log.Logger = log.With().Caller().Logger()
	}
}

func getConfig(filePath string) config.Config {
	conf, err := config.Load(filePath)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to load config file")
	}
	return conf
}

func main() {
	configFile := os.Args[1]
	conf := getConfig(configFile)
	db := conf.Database
	dataTransport := conf.Transport

	setupLogs(conf.Logs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err := db.Open()
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to connect to database")
	}
	defer db.Close()

	// go dataTransport.StartProducer()
	go dataTransport.StartConsumer()

	commands := make([]cmd.Command, 0)
	if conf.API.Enabled {
		commands = append(commands, api.NewAPIServer(conf.API, db, dataTransport))
	}

	if len(commands) == 0 {
		log.Fatal().Msg("No services are enabled in config file")
	}

	for _, command := range commands {
		go func(command cmd.Command) {
			err := command.Start()
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to start service")
			}
		}(command)
	}

	select {
	case <-ctx.Done():
		for _, command := range commands {
			command.Stop()
		}

		// dataTransport.StopProducer()
		dataTransport.StopConsumer()
	}
}
