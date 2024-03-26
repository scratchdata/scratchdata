package view

import (
	"github.com/go-chi/chi/v5"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
)

func GetView() (*chi.Mux, error) {
	m, err := NewManager(templates.Templates)
	if err != nil {
		return nil, err
	}
	return m.BuildRouter()
}
