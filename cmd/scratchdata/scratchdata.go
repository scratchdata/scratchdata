package scratchdata

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/datasink"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore"
	"github.com/scratchdata/scratchdata/pkg/storage/cache"
	"github.com/scratchdata/scratchdata/pkg/storage/queue"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/api"
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

func GetStorageServices(c config.ScratchDataConfig) (*models.StorageServices, error) {
	rc := &models.StorageServices{}

	blobStore, err := blobstore.NewBlobStore(c.BlobStore)
	if err != nil {
		return nil, err
	}
	rc.BlobStore = blobStore

	queue, err := queue.NewQueue(c.Queue)
	if err != nil {
		return nil, err
	}
	rc.Queue = queue

	cache, err := cache.NewCache(c.Cache)
	if err != nil {
		return nil, err
	}
	rc.Cache = cache

	db := database.NewDatabaseConnection(c.Database, c.Destinations, c.APIKeys)
	rc.Database = db

	return rc, nil
}

func GetMux(storageServices *models.StorageServices, destinationManager *destinations.DestinationManager, dataSink datasink.DataSink) (*chi.Mux, error) {
	apiFunctions, err := api.NewScratchDataAPI(storageServices, destinationManager, dataSink)
	if err != nil {
		log.Error().Err(err).Msg("Unable to start API")
		return nil, err
	}

	mux := api.CreateMux(apiFunctions)
	return mux, nil
}

func Run(config config.ScratchDataConfig, storageServices *models.StorageServices, destinationManager *destinations.DestinationManager, dataSink datasink.DataSink, mux *chi.Mux) {
	setupLogs(config.Logging)

	log.Debug().Msg("Starting Scratch Data")

	ctx, cancel := context.WithCancel(context.Background())

	// Use a WaitGroup to wait for goroutines to finish
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := dataSink.Start(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Unable to start datasink")
		}
	}()

	// Run API
	if config.API.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			api.RunAPI(ctx, config.API, mux)
		}()
	}

	// Run workers
	if config.Workers.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workers.RunWorkers(ctx, config.Workers, storageServices, destinationManager)
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

	if config.Prometheus.Enabled {
		go func() {
			r := chi.NewRouter()

			if config.Prometheus.Username != "" {
				r.Use(middleware.BasicAuth("", map[string]string{config.Prometheus.Username: config.Prometheus.Password}))
			}

			r.Handle("/metrics", promhttp.Handler())
			http.ListenAndServe(fmt.Sprintf(":%d", config.Prometheus.Port), r)
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	destinationManager.CloseAll()

	log.Debug().Msg("Done")
}
