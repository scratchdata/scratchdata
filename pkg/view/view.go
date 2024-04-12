package view

import (
	"encoding/json"
	"html/template"
	"net/http"

	"github.com/foolin/goview"
	"github.com/gorilla/csrf"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/view/session"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

type ShareQuery struct {
	Expires string
	Name    string
	ID      string
}

type LayoutData struct {
	CSRFToken   template.HTML
	Email       string
	HideSidebar bool
	Flashes     []any
	Data        any
}

type View struct {
	gv       *goview.ViewEngine
	sessions *session.Service
}

func NewView(sessions *session.Service, liveReload bool) *View {
	gv := newViewEngine(liveReload)
	return &View{
		gv:       gv,
		sessions: sessions,
	}
}

func (s *View) Render(w http.ResponseWriter, r *http.Request, statusCode int, name string, data any) {
	flashes, err := s.sessions.GetFlashes(w, r)
	if err != nil {
		log.Err(err).Msg("failed to clear flashes")
	}

	m := LayoutData{
		CSRFToken: csrf.TemplateField(r),
		Flashes:   flashes,
	}

	user, ok := session.GetUser(r.Context())
	if !ok {
		return
	}
	m.Email = user.Email
	m.Data = data

	if err := s.gv.Render(w, statusCode, name, m); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
