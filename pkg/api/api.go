package api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage"
)

type ScratchDataAPIStruct struct {
	storageServices storage.StorageServices
	snow            *snowflake.Node
}

func NewScratchDataAPI(storageServices storage.StorageServices) (*ScratchDataAPIStruct, error) {
	// Get the current hostname
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// Hash the hostname using SHA-256
	hash := sha256.Sum256([]byte(hostname))

	// Convert the last byte of the hash to uint32, but we only need the lower 10 bits
	// Note: The hash is a byte array, and we are only working with the last byte for simplicity
	lastByte := hash[len(hash)-1]          // Get the last byte of the hash
	lower10Bits := int64(lastByte) & 0x3FF // Mask to get lower 10 bits

	node, err := snowflake.NewNode(lower10Bits)
	if err != nil {
		return nil, err
	}

	rc := ScratchDataAPIStruct{
		storageServices: storageServices,
		snow:            node,
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
		keyDetails, err := a.storageServices.Database().GetAPIKeyDetails(apiKey)

		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte("Unauthorized"))
			return
		}

		ctx := context.WithValue(r.Context(), "databaseId", keyDetails.DestinationID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *ScratchDataAPIStruct) AuthGetDatabaseID(ctx context.Context) int64 {
	return ctx.Value("databaseId").(int64)
}

func CreateMux(apiFunctions ScratchDataAPI) *chi.Mux {
	r := chi.NewRouter()
	r.Use(apiFunctions.AuthMiddleware)

	api := chi.NewRouter()
	api.Post("/data/{table}", apiFunctions.Insert)
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

	log.Debug().Msg("Waiting for graceful shutdown")
	<-serverCtx.Done()

	log.Debug().Msg("API server stopped")
}
