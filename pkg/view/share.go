package view

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
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
	r.Get("/share/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		queryUUID := chi.URLParam(r, "uuid")

		id, err := uuid.Parse(queryUUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cachedQuery, found := storageServices.Database.GetShareQuery(r.Context(), id)
		if !found {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		year, month, day := cachedQuery.ExpiresAt.Date()

		m := Model{
			HideSidebar: true,
			ShareQuery: ShareQuery{
				Expires: fmt.Sprintf("%s %d, %d", month.String(), day, year),
				ID:      id.String(),
				Name:    cachedQuery.Name,
			},
		}
		if err := gv.Render(w, http.StatusOK, "pages/share", m); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/share/{uuid}/download", func(w http.ResponseWriter, r *http.Request) {
		format := strings.ToLower(r.URL.Query().Get("format"))

		queryUUID := chi.URLParam(r, "uuid")

		id, err := uuid.Parse(queryUUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cachedQuery, found := storageServices.Database.GetShareQuery(r.Context(), id)
		if !found {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		dest, err := destinationManager.Destination(r.Context(), cachedQuery.DestinationID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		switch format {
		case "csv":
			w.Header().Set("Content-Type", "text/csv")
			if err := dest.QueryCSV(cachedQuery.Query, w); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			if err := dest.QueryJSON(cachedQuery.Query, w); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})
}
