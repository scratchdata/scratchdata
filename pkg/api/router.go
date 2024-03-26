package api

import (
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth/v5"
	"github.com/scratchdata/scratchdata/pkg/view"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var latency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "latency",
	Help:    "Request latency",
	Buckets: prometheus.ExponentialBucketsRange(.05, 30, 20),
}, []string{"route", "status_code"})

var responseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bytes_returned",
	Help:    "Bytes returned",
	Buckets: prometheus.ExponentialBucketsRange(1000, 100_000_000, 20),
}, []string{"route"})

func CreateMux(apiFunctions *ScratchDataAPIStruct) *chi.Mux {
	r := chi.NewRouter()
	r.Use(PrometheusMiddleware)
	r.Get("/share/{uuid}/data.{format}", apiFunctions.ShareData) // New endpoint for sharing data

	api := chi.NewRouter()
	api.Use(apiFunctions.AuthMiddleware)
	api.Post("/data/insert/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)
	api.Post("/data/query", apiFunctions.Select)

	api.Get("/destinations", apiFunctions.GetDestinations)
	api.Post("/destinations", apiFunctions.CreateDestination)
	api.Post("/destinations/{id}/keys", apiFunctions.AddAPIKey)
	api.Post("/data/query/share", apiFunctions.CreateQuery) // New endpoint for creating a query

	r.Mount("/api", api)

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Use(jwtauth.Verifier(apiFunctions.tokenAuth))

	// TODO breadchris renable auth
	//router.Use(apiFunctions.Authenticator(apiFunctions.tokenAuth))

	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "PUT", "POST", "DELETE", "HEAD", "OPTION"},
		AllowedHeaders:   []string{"User-Agent", "Content-Type", "Accept", "Accept-Encoding", "Accept-Language", "Cache-Control", "Connection", "DNT", "Host", "Origin", "Pragma", "Referer"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	}))

	router.Group(func(r chi.Router) {
		r.Get("/login", apiFunctions.Login)
		r.Get("/logout", apiFunctions.Logout)
		r.Get("/oauth/{provider}/callback", apiFunctions.OAuthCallback)
	})

	router.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})
	router.Mount("/dashboard/", view.GetDashboard())
	r.Mount("/", router)
	return r
}