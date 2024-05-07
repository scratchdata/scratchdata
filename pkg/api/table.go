package api

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func (a *ScratchDataAPIStruct) Tables(w http.ResponseWriter, r *http.Request) {
	databaseIDParam := chi.URLParam(r, "destination")
	destId, err := strconv.ParseInt(databaseIDParam, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dest, err := a.destinationManager.Destination(r.Context(), destId)
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

	databaseIDParam := chi.URLParam(r, "destination")
	destId, err := strconv.ParseInt(databaseIDParam, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dest, err := a.destinationManager.Destination(r.Context(), destId)
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
