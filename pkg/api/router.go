package api

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/jwtauth/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/view/static"
)

//go:embed dist/*
var spaFiles embed.FS

var latency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "latency",
	Help:    "ConnRequest latency",
	Buckets: prometheus.ExponentialBucketsRange(.05, 30, 20),
}, []string{"route", "status_code"})

var responseSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "bytes_returned",
	Help:    "Bytes returned",
	Buckets: prometheus.ExponentialBucketsRange(1000, 100_000_000, 20),
}, []string{"route"})

func SPAHandler(prefix string) http.HandlerFunc {
	spaFS, err := fs.Sub(spaFiles, "dist")
	if err != nil {
		panic(fmt.Errorf("failed getting the sub tree for the site files: %w", err))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		cleaned := path.Clean(r.URL.Path)
		p := strings.TrimPrefix(cleaned, prefix)
		// p := strings.TrimPrefix(cleaned, "/")
		// p := strings.TrimPrefix(cleaned, "/dashboard/")
		// p := cleaned

		f, err := spaFS.Open(p)
		if err == nil {
			defer f.Close()
		}

		// if os.IsNotExist(err) {
		if err != nil {
			r.URL.Path = "/"
			// } else if p == "/dashboard" {
			// r.URL.Path = "/"
		} else {
			r.URL.Path = p
		}

		http.FileServer(http.FS(spaFS)).ServeHTTP(w, r)
	}
}

func CreateMux(
	storageServices *storage.Services,
	apiFunctions *ScratchDataAPIStruct,
	c config.ScratchDataConfig,
	destinationManager *destinations.DestinationManager,
) *chi.Mux {
	r := chi.NewRouter()
	r.Use(PrometheusMiddleware)
	r.Use(jwtauth.Verifier(apiFunctions.tokenAuth))

	r.Get("/healthcheck", apiFunctions.Healthcheck)

	r.Get("/share/{uuid}/data.{format}", apiFunctions.ShareData)

	api := chi.NewRouter()
	api.Use(apiFunctions.AuthMiddleware)

	api.Get("/data/{destination}/query", apiFunctions.Select)
	api.Post("/data/{destination}/insert/{table}", apiFunctions.Insert)
	api.Post("/data/{destination}/query", apiFunctions.Select)
	api.Post("/data/{source}/copy", apiFunctions.Copy)
	api.Post("/data/{destination}/query/share", apiFunctions.CreateQuery)
	api.Delete("/data/{destination}", apiFunctions.DeleteDestination)

	api.Get("/tables", apiFunctions.Tables)
	api.Get("/tables/{table}/columns", apiFunctions.Columns)

	api.Get("/destinations/params/{type}", apiFunctions.GetDestinationParams)
	api.Get("/destinations", apiFunctions.GetDestinations)
	api.Post("/destinations", apiFunctions.CreateDestination)
	api.Post("/keys", apiFunctions.AddAPIKey)

	r.Mount("/api", api)

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	// router.Use(jwtauth.Verifier(apiFunctions.tokenAuth))

	// router.Use(cors.Handler(cors.Options{
	// 	AllowedOrigins:   []string{"*"},
	// 	AllowedMethods:   []string{"GET", "PUT", "POST", "DELETE", "HEAD", "OPTION"},
	// 	AllowedHeaders:   []string{"User-Agent", "Content-Type", "Accept", "Accept-Encoding", "Accept-Language", "Cache-Control", "Connection", "DNT", "Host", "Origin", "Pragma", "Referer"},
	// 	ExposedHeaders:   []string{"Link"},
	// 	AllowCredentials: true,
	// 	MaxAge:           300, // Maximum value not ignored by any of major browsers
	// }))

	router.Get("/login", apiFunctions.Login)
	router.Get("/logout", apiFunctions.Logout)
	router.Get("/oauth/{provider}/callback", apiFunctions.OAuthCallback)

	if c.Dashboard.Enabled {
		fileServer := http.FileServer(http.FS(static.Static))

		webRouter := chi.NewRouter()

		webRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
		})

		webRouter.Handle("/static/*", http.StripPrefix("/static", fileServer))
		webRouter.Handle("/", SPAHandler("/"))
		webRouter.Handle("/*", SPAHandler("/"))

		// dashboardRouter := chi.NewRouter()
		// dashboardRouter.Use(apiFunctions.AuthMiddleware)
		// dashboardRouter.Use(apiFunctions.DashboardAuthMiddleware())

		webRouter.With(apiFunctions.AuthMiddleware).With(apiFunctions.DashboardAuthMiddleware()).Handle("/dashboard/*", apiFunctions.DashboardAuthMiddleware()(SPAHandler("/dashboard")))
		webRouter.With(apiFunctions.AuthMiddleware).With(apiFunctions.DashboardAuthMiddleware()).Handle("/dashboard", apiFunctions.DashboardAuthMiddleware()(SPAHandler("/dashboard")))

		router.Mount("/", webRouter)

	}

	// err := view.MountRoutes(
	// 	router,
	// 	storageServices,
	// 	c.Dashboard,
	// 	destinationManager,
	// 	apiFunctions.Authenticator(),
	// )
	// if err != nil {
	// 	panic(err)
	// }
	r.Mount("/", router)
	return r
}
