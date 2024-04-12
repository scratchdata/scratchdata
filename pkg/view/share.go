package view

import (
	"github.com/go-chi/chi/v5"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
)

func RegisterShareView(
	r *chi.Mux,
	storageServices *storage.Services,
	destinationManager *destinations.DestinationManager,
	c config.DashboardConfig,
) {
	gv := newViewEngine(c.LiveReload)
}
