package api

import (
	"net/http"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth/v5"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/view"
	"github.com/scratchdata/scratchdata/pkg/view/static"

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

func CreateMux(
	storageServices *storage.Services,
	apiFunctions *ScratchDataAPIStruct,
	c config.ScratchDataConfig,
	destinationManager *destinations.DestinationManager,
) *chi.Mux {
	r := chi.NewRouter()
	r.Use(PrometheusMiddleware)
	r.Get("/healthcheck", apiFunctions.Healthcheck)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})

	r.Get("/share/{uuid}/data.{format}", apiFunctions.ShareData)

	api := chi.NewRouter()
	api.Use(apiFunctions.AuthMiddleware)
	api.Post("/data/insert/{table}", apiFunctions.Insert)
	api.Get("/data/query", apiFunctions.Select)
	api.Post("/data/query", apiFunctions.Select)
	api.Post("/data/copy", apiFunctions.Copy)
	api.Get("/tables", apiFunctions.Tables)
	api.Get("/tables/{table}/columns", apiFunctions.Columns)

	api.Get("/destinations", apiFunctions.GetDestinations)
	api.Post("/destinations", apiFunctions.CreateDestination)
	api.Post("/destinations/{id}/keys", apiFunctions.AddAPIKey)
	api.Post("/data/query/share", apiFunctions.CreateQuery)

	r.Mount("/api", api)

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Use(jwtauth.Verifier(apiFunctions.tokenAuth))

	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "PUT", "POST", "DELETE", "HEAD", "OPTION"},
		AllowedHeaders:   []string{"User-Agent", "Content-Type", "Accept", "Accept-Encoding", "Accept-Language", "Cache-Control", "Connection", "DNT", "Host", "Origin", "Pragma", "Referer"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300, // Maximum value not ignored by any of major browsers
	}))

	router.Get("/login", apiFunctions.Login)
	router.Get("/logout", apiFunctions.Logout)
	router.Get("/oauth/{provider}/callback", apiFunctions.OAuthCallback)

	router.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
	})

	if c.Dashboard.Enabled {
		d, err := view.New(storageServices, c.Dashboard, destinationManager, apiFunctions.Authenticator(apiFunctions.tokenAuth))
		if err != nil {
			panic(err)
		}

		fileServer := http.FileServer(http.FS(static.Static))
		router.Handle("/static/*", http.StripPrefix("/static", fileServer))

		router.Mount("/dashboard", d)
	}

	r.Mount("/", router)
	return r
}
