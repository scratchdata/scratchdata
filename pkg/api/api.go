package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/scratchdata/scratchdata/models"
	"github.com/scratchdata/scratchdata/pkg/datasink"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage/cache"
	"github.com/scratchdata/scratchdata/util"

	"github.com/bwmarrin/snowflake"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
)

type QueryStore map[string]time.Time

type ScratchDataAPIStruct struct {
	storageServices    *models.StorageServices
	destinationManager *destinations.DestinationManager
	dataSink           datasink.DataSink
	snow               *snowflake.Node
	cache              cache.Cache
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
		cache:              *cache.NewCache(),
	}

	return &rc, nil
}

type ScratchDataAPI interface {
	Select(w http.ResponseWriter, r *http.Request)
	Insert(w http.ResponseWriter, r *http.Request)
	CreateQuery(w http.ResponseWriter, r *http.Request)
	ShareData(w http.ResponseWriter, r *http.Request)

	AuthMiddleware(next http.Handler) http.Handler
	AuthGetDatabaseID(context.Context) int64
}

func (a *ScratchDataAPIStruct) AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.URL.Query().Get("api_key")
		keyDetails, err := a.storageServices.Database.GetAPIKeyDetails(apiKey)

		if err != nil && !strings.HasPrefix(r.URL.Path, "/api/share") {
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

func (a *ScratchDataAPIStruct) CreateQuery(w http.ResponseWriter, r *http.Request) {
	apiKey := r.URL.Query().Get("api_key")
	if apiKey != "local" { // Validate API key
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte("Unauthorized"))
		return
	}

	var requestBody struct {
		Query    string `json:"query"`
		Duration int    `json:"duration"` // Duration in seconds
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request body"))
		return
	}

	// Validate the query
	if requestBody.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Query cannot be empty"))
		return
	}

	// Generate a new UUID
	queryUUID := uuid.New()

	// Store the query and its expiration time
	queryExpiration := time.Duration(requestBody.Duration)
	a.cache.Set(queryUUID.String(), []byte(requestBody.Query), &queryExpiration)

	// Return the UUID representing the query
	w.Header().Set("Content-Type", "application/json")
	response := struct {
		QueryUUID string `json:"query_uuid"`
	}{
		QueryUUID: queryUUID.String(),
	}
	json.NewEncoder(w).Encode(response)
}

func (a *ScratchDataAPIStruct) executeQueryAndStreamData(w http.ResponseWriter, ctx context.Context, query string, databaseID int64, format string) error {
	dest, err := a.destinationManager.Destination(databaseID)
	if err != nil {
		return err
	}

	switch strings.ToLower(format) {
	case "csv":
		w.Header().Set("Content-Type", "text/csv")
		return dest.QueryCSV(query, w)
	default:
		w.Header().Set("Content-Type", "application/json")
		return dest.QueryJSON(query, w)
	}
}

func (a *ScratchDataAPIStruct) ShareData(w http.ResponseWriter, r *http.Request) {
	queryUUID := chi.URLParam(r, "uuid")
	format := chi.URLParam(r, "format")

	// Retrieve query from cache using UUID
	query, found := a.cache.Get(queryUUID)
	if !found {
		http.Error(w, "Query not found", http.StatusNotFound)
		return
	}

	// Convert query to string
	queryStr := string(query)

	// Execute query and stream data
	databaseID := a.AuthGetDatabaseID(r.Context())
	if err := a.executeQueryAndStreamData(w, r.Context(), queryStr, databaseID, format); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func CreateMux(apiFunctions ScratchDataAPI) *chi.Mux {
	r := chi.NewRouter()
	r.Use(apiFunctions.AuthMiddleware)

	api := chi.NewRouter()
	api.Post("/data/insert/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)
	api.Post("/data/query", apiFunctions.Select)
	api.Post("/data/query/share", apiFunctions.CreateQuery)        // New endpoint for creating a query
	api.Get("/share/{uuid}/data.{format}", apiFunctions.ShareData) // New endpoint for sharing data

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
