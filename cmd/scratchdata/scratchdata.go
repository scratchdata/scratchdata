package scratchdata

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/api"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
	"github.com/scratchdata/scratchdata/pkg/workers"
)

func setupLogs(logConfig config.Logging) {
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
	logLevel := zerolog.TraceLevel
	switch logConfig.Level {
	case "panic":
		logLevel = zerolog.PanicLevel
	case "fatal":
		logLevel = zerolog.FatalLevel
	case "error":
		logLevel = zerolog.ErrorLevel
	case "warn":
		logLevel = zerolog.WarnLevel
	case "info":
		logLevel = zerolog.InfoLevel
	case "debug":
		logLevel = zerolog.DebugLevel
	case "trace":
		logLevel = zerolog.TraceLevel
	}
	zerolog.SetGlobalLevel(logLevel)

	// Set log output format
	if logConfig.JSONFormat {
		log.Logger = log.With().Caller().Logger()
	} else {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"}).With().Caller().Logger()
	}
}

func GetStorageServices(c config.ScratchDataConfig) storage.StorageServices {
	// TODO: NewBlobStore(config)
	var blobStore storage.BlobStoreI
	switch c.BlobStore.Type {
	default:
	}

	var queue storage.QueueI
	switch c.Queue.Type {
	default:
	}

	var cache storage.CacheI
	switch c.Cache.Type {
	default:
	}

	db := database.NewDatabaseConnection(c.Database, c.Destinations)

	var dataSink storage.DataSink
	switch c.DataSink.Type {
	default:
	}

	rc := storage.NewStorageService(db, cache, queue, blobStore, dataSink)
	return rc
}

func Run(config config.ScratchDataConfig, storageServices storage.StorageServices) {
	setupLogs(config.Logging)

	log.Debug().Msg("Starting Scratch Data")

	ctx, cancel := context.WithCancel(context.Background())

	// Use a WaitGroup to wait for goroutines to finish
	var wg sync.WaitGroup

	// Run API
	if config.API.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()

			apiFunctions, err := api.NewScratchDataAPI(storageServices)
			if err != nil {
				log.Error().Err(err).Msg("Unable to start API")
				return
			}

			mux := api.CreateMux(apiFunctions)
			api.RunAPI(ctx, config.API, mux)
		}()
	}

	// Run workers
	if config.Workers.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workers.RunWorkers(ctx, config.Workers, storageServices)
		}()
	}

	// Set up channel to listen for SIGINT (Ctrl+C) and SIGTERM (kill command)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)

	// Block until a signal is received
	go func() {
		sig := <-sigs
		log.Debug().Str("signal", sig.String()).Msg("Received signal, stopping")
		// Cancel the context, signaling all goroutines to shut down
		cancel()
	}()

	// Wait for all goroutines to finish
	wg.Wait()
	log.Debug().Msg("Done")
}
