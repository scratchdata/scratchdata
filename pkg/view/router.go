package view

import (
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/csrf"
	"github.com/gorilla/sessions"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/connections"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/view/session"
	"github.com/scratchdata/scratchdata/pkg/view/static"
)

func MountRoutes(
	r chi.Router,
	storageServices *storage.Services,
	c config.DashboardConfig,
	destManager *destinations.DestinationManager,
	auth func(h http.Handler) http.Handler,
) error {
	csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))
	sessionStore := sessions.NewCookieStore([]byte(c.CSRFSecret))

	sessionService := session.NewSession(sessionStore)
	view, err := NewView(sessionService, c.LiveReload)
	if err != nil {
		return err
	}

	connService := connections.NewService(
		c,
		storageServices,
		destManager,
	)
	controller := NewController(
		sessionService,
		connService,
		view,
	)

	r.Get("/share/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		queryUUID := chi.URLParam(r, "uuid")

		id, err := uuid.Parse(queryUUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cachedQuery, found := storageServices.Database.GetPublicQuery(r.Context(), id)
		if !found {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		year, month, day := cachedQuery.ExpiresAt.Date()

		res := ShareQuery{
			Expires: fmt.Sprintf("%s %d, %d", month.String(), day, year),
			ID:      id.String(),
			Name:    cachedQuery.Name,
		}
		view.RenderExternal(w, r, http.StatusOK, "pages/share", res)
	})

	r.HandleFunc("/{path:.*}", func(w http.ResponseWriter, r *http.Request) {
		p := chi.URLParam(r, "path")
		if p == "" {
			return
		}

		params := map[string]string{}
		for _, qp := range r.URL.Query() {
			// XXX breadchris support arrays
			if len(qp) == 1 {
				param := qp[0]
				params[param] = r.URL.Query().Get(param)
			}
		}
	})

	if c.Enabled {
		fileServer := http.FileServer(http.FS(static.Static))
		r.Handle("/static/*", http.StripPrefix("/static", fileServer))

		r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
		})
		r.Mount("/request", controller.RequestRoutes(csrfMiddleware))
		r.Route("/dashboard", func(r chi.Router) {
			r.Mount("/", controller.HomeRoute(auth))
			r.Mount("/query", controller.QueryRoutes(auth, csrfMiddleware))
			r.Mount("/connections", controller.ConnRoutes(auth, csrfMiddleware))
		})
	}
	return nil
}
