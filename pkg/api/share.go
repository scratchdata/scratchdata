package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
)

type CachedQueryData struct {
	Query      string
	DatabaseID int64
}

func (a *ScratchDataAPIStruct) CreateQuery(w http.ResponseWriter, r *http.Request) {
	var requestBody struct {
		Query    string `json:"query"`
		Duration int    `json:"duration"` // Duration in seconds
		Name     string `json:"name"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid request body"))
		return
	}

	// Validate the query
	if requestBody.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Query cannot be empty"))
		return
	}

	if requestBody.Name == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Name cannot be empty"))
		return
	}

	destId := a.AuthGetDatabaseID(r.Context())
	expires := time.Duration(requestBody.Duration) * time.Second
	sharedQueryId, err := a.storageServices.Database.CreateShareQuery(r.Context(), destId, requestBody.Name, requestBody.Query, expires)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	render.JSON(w, r, render.M{"id": sharedQueryId.String()})
}

func (a *ScratchDataAPIStruct) ShareData(w http.ResponseWriter, r *http.Request) {
	queryUUID := chi.URLParam(r, "uuid")
	format := chi.URLParam(r, "format")

	id, err := uuid.Parse(queryUUID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cachedQuery, found := a.storageServices.Database.GetShareQuery(r.Context(), id)
	if !found {
		http.Error(w, "Query not found", http.StatusNotFound)
		return
	}

	if err := a.executeQueryAndStreamData(r.Context(), w, cachedQuery.Query, cachedQuery.DestinationID, format); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
