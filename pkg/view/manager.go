package view

import (
	"github.com/fsnotify/fsnotify"
	"github.com/go-chi/chi/v5"
	"github.com/go-faster/errors"
	"github.com/rs/zerolog/log"
	"html/template"
	"io/fs"
	"net/http"
	"path/filepath"
)

type TemplateHandler func(w http.ResponseWriter, r *http.Request) any

type TemplateRoute struct {
	name    string
	handler func(w http.ResponseWriter, r *http.Request) any
	tmpl    *template.Template
}

type Manager struct {
	templates map[string]*TemplateRoute
	tfs       fs.FS
	watcher   *fsnotify.Watcher
}

func NewManager(fsys fs.FS) (*Manager, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file watcher")
	}

	m := &Manager{
		templates: make(map[string]*TemplateRoute),
		tfs:       fsys,
		watcher:   watcher,
	}
	err = m.loadTemplates()
	if err != nil {
		return nil, errors.Wrap(err, "failed to load templates")
	}

	// TODO breadchris setup fs different in dev for live reload
	//go m.watchTemplateChanges()
	return m, nil
}

func (s *Manager) loadTemplates() error {
	return fs.WalkDir(s.tfs, "pages", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		return s.registerTemplate(path)
	})
}

func (s *Manager) watchTemplateChanges() {
	for {
		select {
		case event, ok := <-s.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				err := s.registerTemplate(event.Name)
				if err != nil {
					log.Error().Err(err).Msg("failed to register template")
				}
			}
		case err, ok := <-s.watcher.Errors:
			if !ok {
				return
			}
			log.Error().Err(err).Msg("watcher error")
		}
	}
}

func (s *Manager) registerTemplate(p string) error {
	relPath, err := filepath.Rel("pages", p)
	if err != nil {
		return errors.Wrapf(err, "failed to get relative path for %s", p)
	}

	// split file name from path
	b := filepath.Base(relPath)
	if b == "index.html" {
		relPath = filepath.Dir(relPath)
	} else {
		relPath = relPath[:len(relPath)-len(filepath.Ext(relPath))]
	}

	urlPath := "/" + filepath.ToSlash(relPath)
	if relPath == "." {
		urlPath = "/"
	}

	tmpl, err := template.ParseFS(s.tfs, "layout/base.html", p)
	if err != nil {
		return errors.Wrapf(err, "failed to parse template %s", p)
	}

	s.templates[urlPath] = &TemplateRoute{
		name:    relPath,
		handler: nil,
		tmpl:    tmpl,
	}

	// TODO breadchris need setup fs different in dev for live reload
	//return s.watcher.Add(path)
	return nil
}

func (s *Manager) BuildRouter() (*chi.Mux, error) {
	mux := chi.NewMux()
	for path, route := range s.templates {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			var d any
			if route.handler != nil {
				d = route.handler(w, r)
			}
			if err := route.tmpl.Execute(w, d); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		})
	}
	return mux, nil
}
