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

	cachedQueryData := CachedQueryData{
		Query:      requestBody.Query,
		DatabaseID: a.AuthGetDatabaseID(r.Context()),
	}
	cachedQueryDataBytes, err := json.Marshal(cachedQueryData)

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to marshal data"))
		return
	}

	// Generate a new UUID
	queryUUID := uuid.New()

	// Store the query and its expiration time
	queryExpiration := time.Duration(requestBody.Duration)
	a.storageServices.Cache.Set(queryUUID.String(), cachedQueryDataBytes, &queryExpiration)

	// Return the UUID representing the query
	render.JSON(w, r, render.M{"id": queryUUID.String()})
}

func (a *ScratchDataAPIStruct) ShareData(w http.ResponseWriter, r *http.Request) {
	queryUUID := chi.URLParam(r, "uuid")
	format := chi.URLParam(r, "format")

	// Retrieve query from cache using UUID
	cachedQueryDataBytes, found := a.storageServices.Cache.Get(queryUUID)
	if !found {
		http.Error(w, "Query not found", http.StatusNotFound)
		return
	}

	var cachedQueryData CachedQueryData
	if err := json.Unmarshal(cachedQueryDataBytes, &cachedQueryData); err != nil {
		http.Error(w, "Failed to unmarshal data", http.StatusInternalServerError)
		return
	}

	// Convert query to string
	queryStr := cachedQueryData.Query

	// Execute query and stream data
	databaseID := cachedQueryData.DatabaseID
	if err := a.executeQueryAndStreamData(w, queryStr, databaseID, format); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
