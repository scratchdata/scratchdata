package api

import (
	"encoding/json"
	"github.com/scratchdata/scratchdata/pkg/config"
	"net/http"

	"github.com/go-chi/render"
	"github.com/google/uuid"
)

func (a *ScratchDataAPIStruct) AddAPIKey(w http.ResponseWriter, r *http.Request) {
	key := uuid.New().String()
	destId := a.AuthGetDatabaseID(r.Context())
	hashedKey := a.storageServices.Database.Hash(key)
	a.storageServices.Database.AddAPIKey(r.Context(), int64(destId), hashedKey)

	render.JSON(w, r, render.M{"key": key, "destination_id": destId})
}

func (a *ScratchDataAPIStruct) GetDestinations(w http.ResponseWriter, r *http.Request) {
	dest := a.storageServices.Database.GetDestinations(r.Context())
	for i := range dest {
		dest[i].APIKeys = nil
		dest[i].Settings = nil
	}
	render.JSON(w, r, dest)
}

func (a *ScratchDataAPIStruct) CreateDestination(w http.ResponseWriter, r *http.Request) {
	dest := config.Destination{}
	decoder := json.NewDecoder(r.Body)

	err := decoder.Decode(&dest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = a.destinationManager.TestCredentials(dest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newDest, err := a.storageServices.Database.CreateDestination(r.Context(), dest.Type, dest.Settings)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.PlainText(w, r, err.Error())
		return
	}

	newDest.Settings = nil
	render.JSON(w, r, newDest)
}
