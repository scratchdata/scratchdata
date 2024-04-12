package view

import (
	"encoding/gob"
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/csrf"
	"github.com/gorilla/schema"
	"github.com/gorilla/sessions"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/connections"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view/model"
	"github.com/scratchdata/scratchdata/pkg/view/request"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type Connections struct {
	Destinations []config.Destination
}

type UpsertConnection struct {
	RequestID   string
	Destination config.Destination
	TypeDisplay string
	FormFields  []util.Form
}

type Connect struct {
	APIKey string
	APIUrl string
}

type ShareQuery struct {
	Expires string
	Name    string
	ID      string
}

type FlashType string

const (
	FlashTypeSuccess FlashType = "success"
	FlashTypeWarning FlashType = "warning"
	FlashTypeError   FlashType = "error"
)

type Flash struct {
	Type    FlashType
	Title   string
	Message string
	Fatal   bool
}

type Request struct {
	URL string
}

type Model struct {
	CSRFToken        template.HTML
	Email            string
	HideSidebar      bool
	Flashes          []Flash
	Connect          Connect
	Connections      Connections
	UpsertConnection UpsertConnection
	Data             map[string]any
	Request          Request
	ShareQuery       ShareQuery
}

func init() {
	gob.Register(Flash{})
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

func New(
	storageServices *storage.Services,
	c config.DashboardConfig,
	destManager *destinations.DestinationManager,
	auth func(h http.Handler) http.Handler,
) (*chi.Mux, error) {
	homeRouter := chi.NewRouter()

	csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))
	sessionStore := sessions.NewCookieStore([]byte(c.CSRFSecret))

	sessionService := NewSession(sessionStore)

	modelLoader := model.NewModelLoader(sessionService)

	connController := connections.NewService()
	reqController := request.NewController()

	homeRouter.Use(auth)

	formDecoder := schema.NewDecoder()
	formDecoder.IgnoreUnknownKeys(true)

	gv := newViewEngine(c.LiveReload)

	homeRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/index", modelLoader.Load(r, w))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r := chi.NewRouter()
	r.Mount("/", homeRouter)
	r.Mount("/request", reqController.NewRouter(csrfMiddleware))
	// TODO: Want to be able to disable this for quick local dev
	r.Mount("/connections", connController.NewRouter(auth, csrfMiddleware))

	return r, nil
}
