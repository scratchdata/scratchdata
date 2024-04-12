package view

import (
	"encoding/json"
	"net/http"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/csrf"
	"github.com/gorilla/sessions"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/connections"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/view/model"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func NewRouter(
	storageServices *storage.Services,
	c config.DashboardConfig,
	destManager *destinations.DestinationManager,
	auth func(h http.Handler) http.Handler,
) (*chi.Mux, error) {
	csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))
	sessionStore := sessions.NewCookieStore([]byte(c.CSRFSecret))

	sessionService := NewSession(sessionStore)
	modelLoader := model.NewModelLoader(sessionService)
	gv := newViewEngine(c.LiveReload)

	connService := connections.NewService(
		c,
		storageServices,
		destManager,
	)
	controller := NewController(
		sessionService,
		connService,
		modelLoader,
		gv,
	)

	r := chi.NewRouter()
	r.Mount("/", controller.NewHomeRouter(auth))
	r.Mount("/request", controller.NewRequestRouter(csrfMiddleware))
	r.Mount("/connections", controller.NewConnRouter(auth, csrfMiddleware))
	return r, nil
}

func embeddedFH(config goview.Config, tmpl string) (string, error) {
	bytes, err := templates.Templates.ReadFile(tmpl + config.Extension)
	return string(bytes), err
}

func newViewEngine(liveReload bool) *goview.ViewEngine {
	gv := goview.New(goview.Config{
		Root:         "pkg/view/templates",
		Extension:    ".html",
		Master:       "layout/base",
		Partials:     []string{"partials/flash"},
		DisableCache: true,
		Funcs: map[string]any{
			"prettyPrint": func(data any) string {
				bytes, err := json.MarshalIndent(data, "", "    ")
				if err != nil {
					return err.Error()
				}
				return string(bytes)
			},
			"title": func(a string) string {
				return cases.Title(language.AmericanEnglish).String(a)
			},
		},
	})
	if !liveReload {
		gv.SetFileHandler(embeddedFH)
	}
	return gv
}
