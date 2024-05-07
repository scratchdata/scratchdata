package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/util"
	"gorm.io/datatypes"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/google/uuid"
)

func (a *ScratchDataAPIStruct) AddAPIKey(w http.ResponseWriter, r *http.Request) {
	apiKey, ok := a.AuthGetAPIKeyDetails(r)
	if !ok {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	newKey := uuid.New().String()
	hashedKey := a.storageServices.Database.Hash(newKey)

	a.storageServices.Database.AddAPIKey(r.Context(), apiKey.TeamID, hashedKey)

	render.JSON(w, r, render.M{"key": newKey})
}

func (a *ScratchDataAPIStruct) DeleteDestination(w http.ResponseWriter, r *http.Request) {
	idStr := r.Form.Get("destination")
	id, err := strconv.ParseUint(idStr, 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: handle error
	teamId, _ := a.AuthGetTeamID(r)

	err = a.storageServices.Database.DeleteDestination(r.Context(), teamId, uint(id))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	render.PlainText(w, r, "ok")
}

func (a *ScratchDataAPIStruct) GetDestinationParams(w http.ResponseWriter, r *http.Request) {
	t := chi.URLParam(r, "type")

	vc, ok := destinations.ViewConfig[t]
	if !ok {
		http.Error(w, "Unknown connection type", http.StatusBadRequest)
		return
	}

	form := util.ConvertToForms(vc.Type)
	render.JSON(w, r, render.M{"type": vc.Display, "form_fields": form})
}

func (a *ScratchDataAPIStruct) GetDestinations(w http.ResponseWriter, r *http.Request) {
	// user, ok := UserFromContext(r.Context())
	// if !ok {
	// 	http.Error(w, "unable to get user", http.StatusInternalServerError)
	// 	return
	// }
	dest, err := a.storageServices.Database.GetDestinations(r.Context(), 1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for i := range dest {
		dest[i].Settings = datatypes.NewJSONType(map[string]any{})
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

	teamId, ok := a.AuthGetTeamID(r)
	if !ok {
		http.Error(w, "unable to get team", http.StatusInternalServerError)
		return
	}
	newDest, err := a.storageServices.Database.CreateDestination(
		r.Context(),
		teamId,
		dest.Name,
		dest.Type,
		dest.Settings,
	)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.PlainText(w, r, err.Error())
		return
	}

	dstCfg := newDest.ToConfig()
	dstCfg.Settings = map[string]any{}

	render.JSON(w, r, dstCfg)
}
