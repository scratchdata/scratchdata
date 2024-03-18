package api

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
)

type ScratchDataAPI interface {
	Select(w http.ResponseWriter, r *http.Request)
	Insert(w http.ResponseWriter, r *http.Request)

	AuthMiddleware(next http.Handler) http.Handler
	AuthGetDatabaseID(context.Context) int64

	GetDestinations(w http.ResponseWriter, r *http.Request)
	CreateDestination(w http.ResponseWriter, r *http.Request)
	AddAPIKey(w http.ResponseWriter, r *http.Request)
}

func CreateMux(apiFunctions ScratchDataAPI) *chi.Mux {
	r := chi.NewRouter()
	r.Use(apiFunctions.AuthMiddleware)

	api := chi.NewRouter()
	api.Post("/data/insert/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)
	api.Post("/data/query", apiFunctions.Select)

	api.Get("/destinations", apiFunctions.GetDestinations)
	api.Post("/destinations", apiFunctions.CreateDestination)
	api.Post("/destinations/{id}/keys", apiFunctions.AddAPIKey)

	r.Mount("/api", api)

	return r
}
