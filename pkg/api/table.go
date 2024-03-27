package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func (a *ScratchDataAPIStruct) Tables(w http.ResponseWriter, r *http.Request) {
	databaseID := a.AuthGetDatabaseID(r.Context())
	dest, err := a.destinationManager.Destination(r.Context(), databaseID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tables, err := dest.Tables()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	render.JSON(w, r, tables)
}

func (a *ScratchDataAPIStruct) Columns(w http.ResponseWriter, r *http.Request) {
	table := chi.URLParam(r, "table")
	databaseID := a.AuthGetDatabaseID(r.Context())

	dest, err := a.destinationManager.Destination(r.Context(), databaseID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	columns, err := dest.Columns(table)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	render.JSON(w, r, columns)
}
