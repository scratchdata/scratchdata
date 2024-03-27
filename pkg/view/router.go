package view

import (
	"net/http"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/templates"
)

type Model struct {
	Email string
}

func embeddedFH(config goview.Config, tmpl string) (string, error) {
	bytes, err := templates.Templates.ReadFile(tmpl + config.Extension)
	return string(bytes), err
}

func New(c config.DashboardConfig, auth func(h http.Handler) http.Handler) (*chi.Mux, error) {
	r := chi.NewRouter()

	// TODO: Want to be able to disable this for quick local dev
	r.Use(auth)

	gv := goview.New(goview.Config{
		Root:         "templates",
		Extension:    ".html",
		Master:       "layout/base",
		DisableCache: true,
	})
	if !c.LiveReload {
		gv.SetFileHandler(embeddedFH)
	}

	loadModel := func(r *http.Request) Model {
		userAny := r.Context().Value("user")
		user, ok := userAny.(*models.User)
		if !ok {
			return Model{}
		}
		return Model{
			Email: user.Email,
		}
	}

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/index", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/connections/index", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections/new", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/connections/new", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/keys", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/keys/index", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return r, nil
}
