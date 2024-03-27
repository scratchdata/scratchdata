package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/datasink"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/util"

	"github.com/bwmarrin/snowflake"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
)

type ScratchDataAPIStruct struct {
	storageServices    *models.StorageServices
	destinationManager *destinations.DestinationManager
	dataSink           datasink.DataSink
	snow               *snowflake.Node
	config             config.API
}

func NewScratchDataAPI(storageServices *models.StorageServices, destinationManager *destinations.DestinationManager, dataSink datasink.DataSink, c config.API) (*ScratchDataAPIStruct, error) {
	snow, err := util.NewSnowflakeGenerator()
	if err != nil {
		return nil, err
	}

	rc := ScratchDataAPIStruct{
		storageServices:    storageServices,
		destinationManager: destinationManager,
		dataSink:           dataSink,
		snow:               snow,
		config:             c,
	}

	return &rc, nil
}

func RunAPI(ctx context.Context, config config.API, mux *chi.Mux) {
	log.Debug().Int("port", config.Port).Msg("Starting API")

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", config.Port),
		Handler: mux,
	}

	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Err(err).Msg("Error serving API")
			serverStopCtx()
		}
	}()

	go func() {
		<-ctx.Done() // Wait for the context to be canceled

		log.Debug().Msg("Stopping API")

		// Gracefully shutdown server
		shutdownCtx, cancel := context.WithTimeout(serverCtx, 30*time.Minute)
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			log.Error().Err(err).Msg("Error shutting down API")
		}

		cancel()
		<-shutdownCtx.Done()

		serverStopCtx()
	}()

	<-serverCtx.Done()

	log.Debug().Msg("API server stopped")
}
