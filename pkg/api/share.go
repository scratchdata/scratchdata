package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
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

	teamId := a.AuthGetTeamID(r.Context())
	destId := a.AuthGetDatabaseID(r.Context())
	expires := time.Duration(requestBody.Duration) * time.Second
	sq := models.NewSavedQuery(
		teamId,
		uint(destId),
		requestBody.Name,
		requestBody.Query,
		expires,
		true,
		"",
	)
	q, err := a.storageServices.Database.UpsertSavedQuery(r.Context(), sq)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	render.JSON(w, r, render.M{"id": q.UUID})
}

func (a *ScratchDataAPIStruct) ShareData(w http.ResponseWriter, r *http.Request) {
	queryUUID := chi.URLParam(r, "uuid")
	format := chi.URLParam(r, "format")

	id, err := uuid.Parse(queryUUID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cachedQuery, found := a.storageServices.Database.GetPublicQuery(r.Context(), id)
	if !found {
		http.Error(w, "Query not found", http.StatusNotFound)
		return
	}

	if err := a.executeQueryAndStreamData(r.Context(), w, cachedQuery.Query, cachedQuery.DestinationID, format, nil); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
