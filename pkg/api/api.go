package api

import (
	"context"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
)

type ScratchDataAPIStruct struct {
	StorageServices config.StorageServices
}

type ScratchDataAPI interface {
	Select(w http.ResponseWriter, r *http.Request)
	Insert(w http.ResponseWriter, r *http.Request)
}

func (a *ScratchDataAPIStruct) Select(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func (a *ScratchDataAPIStruct) Insert(w http.ResponseWriter, r *http.Request) {
}

func CreateMux(apiFunctions ScratchDataAPI) *chi.Mux {
	api := chi.NewRouter()
	api.Post("/data/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)

	r := chi.NewRouter()
	r.Mount("/api", api)

	return r
}

func RunAPI(ctx context.Context, config config.API, mux *chi.Mux) {
	log.Debug().Int("port", config.Port).Msg("Starting API")

	server := &http.Server{
		Addr:    "0.0.0.0:3333",
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
