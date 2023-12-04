package main

import (
	"context"
	"os"
	"os/signal"
	"scratchdata/cmd"
	"scratchdata/cmd/api"
	"scratchdata/config"
	"scratchdata/pkg/accounts"
	"scratchdata/pkg/accounts/dummy"
	"scratchdata/pkg/queue"
	"scratchdata/pkg/storage"
	"scratchdata/pkg/transport"
	"scratchdata/pkg/transport/queuestorage"
	"strconv"
	"syscall"

	"github.com/BurntSushi/toml"
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
	var conf config.Config
	if _, err := toml.DecodeFile(filePath, &conf); err != nil {
		log.Fatal().Err(err).Msg("Unable to load config file")
	}
	return conf
}

func main() {
	configFile := os.Args[1]
	config := getConfig(configFile)

	setupLogs(config.Logs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var accountManager accounts.AccountManager
	accountManager = dummy.DummyAccountManager{}

	var queueBackend queue.QueueBackend
	var storageBackend storage.StorageBackend

	var dataTransport transport.DataTransport
	dataTransport = queuestorage.NewQueueStorageTransport(queueBackend, storageBackend)

	commands := make([]cmd.Command, 0)
	if config.API.Enabled {
		commands = append(commands, api.NewAPIServer(config.API, accountManager, dataTransport))
	}

	if len(commands) == 0 {
		log.Fatal().Msg("No services are enabled in config file")
	}

	for i, _ := range commands {
		go func() {
			err := commands[i].Start()
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to start service")
			}
		}()
	}

	select {
	case <-ctx.Done():
		for _, command := range commands {
			command.Stop()
		}
	}
}
