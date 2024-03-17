package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
}

func NewScratchDataAPI(storageServices *models.StorageServices, destinationManager *destinations.DestinationManager, dataSink datasink.DataSink) (*ScratchDataAPIStruct, error) {
	snow, err := util.NewSnowflakeGenerator()
	if err != nil {
		return nil, err
	}

	rc := ScratchDataAPIStruct{
		storageServices:    storageServices,
		destinationManager: destinationManager,
		dataSink:           dataSink,
		snow:               snow,
	}

	return &rc, nil
}

type ScratchDataAPI interface {
	Select(w http.ResponseWriter, r *http.Request)
	Insert(w http.ResponseWriter, r *http.Request)

	AuthMiddleware(next http.Handler) http.Handler
	AuthGetDatabaseID(context.Context) int64
}

func (a *ScratchDataAPIStruct) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")

		// If we have an admin api key, then get the database_id from a query param
		isAdmin := a.storageServices.Database.VerifyAdminAPIKey(apiKey)
		if isAdmin {
			databaseId := r.URL.Query().Get("database_id")
			dbInt, err := strconv.ParseInt(databaseId, 10, 64)
			if err != nil {
				dbInt = int64(-1)
			}
			ctx := context.WithValue(r.Context(), "databaseId", dbInt)
			next.ServeHTTP(w, r.WithContext(ctx))
		} else {
			// Otherwise, this API key is specific to a user
			keyDetails, err := a.storageServices.Database.GetAPIKeyDetails(apiKey)

			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte("Unauthorized"))
				return
			}

			ctx := context.WithValue(r.Context(), "databaseId", keyDetails.DestinationID)
			next.ServeHTTP(w, r.WithContext(ctx))
		}
	})
}

func (a *ScratchDataAPIStruct) AuthGetDatabaseID(ctx context.Context) int64 {
	return ctx.Value("databaseId").(int64)
}

func CreateMux(apiFunctions ScratchDataAPI) *chi.Mux {
	r := chi.NewRouter()
	r.Use(apiFunctions.AuthMiddleware)

	api := chi.NewRouter()
	api.Post("/data/insert/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)
	api.Post("/data/query", apiFunctions.Select)

	r.Mount("/api", api)

	return r
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
