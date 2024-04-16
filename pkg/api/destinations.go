package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/render"
	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/config"
)

func (a *ScratchDataAPIStruct) AddAPIKey(w http.ResponseWriter, r *http.Request) {
	key := uuid.New().String()
	destId := a.AuthGetDatabaseID(r.Context())
	hashedKey := a.storageServices.Database.Hash(key)
	a.storageServices.Database.AddAPIKey(r.Context(), destId, hashedKey)

	render.JSON(w, r, render.M{"key": key, "destination_id": destId})
}

func (a *ScratchDataAPIStruct) GetDestinations(w http.ResponseWriter, r *http.Request) {
	user, ok := UserFromContext(r.Context())
	if !ok {
		http.Error(w, "unable to get user", http.StatusInternalServerError)
		return
	}
	dest, err := a.storageServices.Database.GetDestinations(r.Context(), user.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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

	teamId := a.AuthGetTeamID(r.Context())
	d, err := a.destinationManager.CreateDestination(r.Context(), teamId, dest)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.PlainText(w, r, err.Error())
		return
	}
	dest.ID = int64(d)
	render.JSON(w, r, dest)
}
