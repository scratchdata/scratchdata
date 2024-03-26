package view

import (
	"embed"
	"html/template"
	"net/http"
	"path/filepath"
	"time"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
)

//go:embed templates/*
var templates embed.FS

type Dashboard struct {
	v *goview.ViewEngine
}

func (d *Dashboard) Index(w http.ResponseWriter, r *http.Request) {
	d.v.Render(w, http.StatusOK, "index", goview.M{})
	// goview.Render(writer, http.StatusOK, "index", goview.M{})
	// render.PlainText(w, r, "hello world")
}

func embeddedFH(config goview.Config, tmpl string) (string, error) {
	path := filepath.Join(config.Root, tmpl)
	bytes, err := templates.ReadFile(path + config.Extension)
	return string(bytes), err
}

func GetDashboard() *chi.Mux {
	gv := goview.New(goview.Config{
		Root:      "templates",
		Extension: ".html",
		Master:    "layout",
		// Partials: []string{"partials/ad"},
		Funcs: template.FuncMap{
			"sub": func(a, b int) int {
				return a - b
			},
			"copy": func() string {
				return time.Now().Format("2006")
			},
		},
		DisableCache: true,
		// Delims:       Delims{Left: "{{", Right: "}}"},
	})
	gv.SetFileHandler(embeddedFH)
	// gv.ViewEngine.SetFileHandler(embeddedFH)

	d := Dashboard{
		v: gv,
	}

	router := chi.NewRouter()
	router.Get("/", d.Index)

	return router
}
