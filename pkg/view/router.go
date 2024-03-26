package view

import (
	"github.com/go-chi/chi/v5"
	"github.com/scratchdata/scratchdata/pkg/storage/database"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
	"html/template"
	"io/fs"
	"net/http"
)

type Model struct {
	Email string
}

func withLayout(f fs.FS, t ...string) (*template.Template, error) {
	base := []string{"layout/base.html"}
	base = append(base, t...)
	return template.ParseFS(f, base...)
}

func New(auth func(h http.Handler) http.Handler) (*chi.Mux, error) {
	r := chi.NewRouter()
	r.Use(auth)

	// TODO breadchris setup reloading of templates
	f := templates.Templates

	loadModel := func(r *http.Request) Model {
		userAny := r.Context().Value("user")
		user, ok := userAny.(*database.User)
		if !ok {
			return Model{}
		}
		return Model{
			Email: user.Email,
		}
	}

	dash, err := withLayout(f, "pages/index.html")
	if err != nil {
		return nil, err
	}
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		err := dash.Execute(w, loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	conns, err := withLayout(f, "pages/connections/index.html")
	if err != nil {
		return nil, err
	}
	r.Get("/connections", func(w http.ResponseWriter, r *http.Request) {
		err := conns.Execute(w, loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connsNew, err := withLayout(f, "pages/connections/new.html")
	if err != nil {
		return nil, err
	}
	r.Get("/connections/new", func(w http.ResponseWriter, r *http.Request) {
		err := connsNew.Execute(w, loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	keys, err := withLayout(f, "pages/keys/index.html")
	if err != nil {
		return nil, err
	}
	r.Get("/keys", func(w http.ResponseWriter, r *http.Request) {
		err := keys.Execute(w, loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return r, nil
}
