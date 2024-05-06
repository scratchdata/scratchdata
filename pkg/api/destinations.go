package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"gorm.io/datatypes"

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
	// user, ok := UserFromContext(r.Context())
	// if !ok {
	// 	http.Error(w, "unable to get user", http.StatusInternalServerError)
	// 	return
	// }
	time.Sleep(1 * time.Second)
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

	userAny := r.Context().Value("user")
	user, ok := userAny.(*models.User)
	if !ok {
		http.Error(w, "unable to get user", http.StatusInternalServerError)
		return
	}
	newDest, err := a.storageServices.Database.CreateDestination(
		r.Context(),
		user.ID,
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
