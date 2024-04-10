package connections

import (
	"errors"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view"
	"gorm.io/datatypes"
)

type FormState struct {
	Name        string
	Type        string
	Settings    map[string]any
	RequestID   string
	HideSidebar bool
}

func (s *Controller) upsertConn(w http.ResponseWriter, r *http.Request, requireRequestID bool) {
	_, err := s.sessions.GetFlashes(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	form := FormState{
		Settings: map[string]any{},
	}

	err = r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := r.Form.Get("id")
	if id != "" {
		http.Error(w, "Editing connections not yet supported", http.StatusBadRequest)
		return
	}

	form.Name = r.Form.Get("name")
	form.Type = r.Form.Get("type")
	form.RequestID = r.Form.Get("request_id")

	var (
		connReq models.ConnectionRequest
	)
	if requireRequestID {
		form.HideSidebar = true

		connReq, err = s.validateRequestId(r.Context(), form.RequestID)
		if err != nil {
			s.flashAndRender(w, r, view.Flash{
				Type:  view.FlashTypeError,
				Title: err.Error(),
			}, form)
			return
		}
	}

	vc, ok := destinations.ViewConfig[form.Type]
	if !ok {
		s.flashAndRender(w, r, view.Flash{
			Type:  view.FlashTypeError,
			Title: "Unknown connection type",
		}, form)
		return
	}

	instance := reflect.New(reflect.TypeOf(vc.Type)).Interface()

	form.Settings = map[string]any{}
	for k, v := range r.PostForm {
		if len(v) == 1 {
			form.Settings[k] = v[0]
		}
	}

	err = s.formDecoder.Decode(instance, r.PostForm)
	if err != nil {
		s.flashAndRender(w, r, view.Flash{
			Type:    view.FlashTypeError,
			Title:   "Failed to decode form",
			Message: err.Error(),
		}, form)
		return
	}

	var settings map[string]any
	err = mapstructure.Decode(instance, &settings)
	if err != nil {
		s.flashAndRender(w, r, view.Flash{
			Type:    view.FlashTypeError,
			Title:   "Failed to decode form",
			Message: err.Error(),
		}, form)
		return
	}

	cd := config.Destination{
		Type:     form.Type,
		Name:     form.Name,
		Settings: settings,
	}

	err = s.destManager.TestCredentials(cd)
	if err != nil {
		log.Err(err).Msg("failed to connect to destination")
		s.flashAndRender(w, r, view.Flash{
			Type:    view.FlashTypeError,
			Title:   "Failed to connect to destination. Check the settings and try again.",
			Message: err.Error(),
		}, form)
		return
	}

	var teamId uint
	if connReq.ID == 0 {
		teamId, err = s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
	} else {
		teamId = connReq.Destination.TeamID
	}

	m := s.modelLoader.Load(r, w)

	if connReq.ID != 0 {
		connReq.Destination.Name = form.Name
		connReq.Destination.Settings = datatypes.NewJSONType(settings)

		err = s.storageServices.Database.UpdateDestination(r.Context(), connReq.Destination)
		if err != nil {
			s.flashAndRender(w, r, view.Flash{
				Type:    view.FlashTypeError,
				Title:   "Failed to update destination",
				Message: err.Error(),
			}, form)
			return
		}

		err = s.storageServices.Database.DeleteConnectionRequest(r.Context(), connReq.ID)
		if err != nil {
			log.Err(err).Msg("failed to delete connection request")
		}

		http.Redirect(w, r, "/dashboard/request/success", http.StatusFound)
		return
	}

	dest, err := s.storageServices.Database.CreateDestination(
		r.Context(), teamId, form.Name, form.Type, settings,
	)
	if err != nil {
		s.flashAndRender(w, r, view.Flash{
			Type:    view.FlashTypeError,
			Title:   "Failed to create destination",
			Message: err.Error(),
		}, form)
		return
	}

	key := uuid.New().String()
	hashedKey := s.storageServices.Database.Hash(key)
	err = s.storageServices.Database.AddAPIKey(r.Context(), int64(dest.ID), hashedKey)
	if err != nil {
		s.flashAndRender(w, r, view.Flash{
			Type:    view.FlashTypeError,
			Title:   "Failed to create destination",
			Message: err.Error(),
		}, form)
		return
	}

	m.Connect.APIKey = key
	m.Connect.APIUrl = s.c.ExternalURL

	err = s.gv.Render(w, http.StatusOK, "pages/connections/api", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) renderNewConnection(w http.ResponseWriter, r *http.Request, m view.Model) {
	err := s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) flashAndRender(w http.ResponseWriter, r *http.Request, f view.Flash, form FormState) {
	log.Info().Interface("flash", f).Msg("failed to create destination")
	s.sessions.NewFlash(w, r, f)

	m := s.modelLoader.Load(r, w)
	if f.Fatal {
		err := s.gv.Render(w, http.StatusInternalServerError, "pages/connections/fatal", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	vc, ok := destinations.ViewConfig[form.Type]
	if !ok {
		http.Error(w, "Unknown connection type", http.StatusBadRequest)
		return
	}

	m.UpsertConnection = view.UpsertConnection{
		Destination: config.Destination{
			ID:       0,
			Name:     form.Name,
			Type:     form.Type,
			Settings: form.Settings,
		},
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
		RequestID:   form.RequestID,
	}
	m.HideSidebar = form.HideSidebar
	s.renderNewConnection(w, r, m)
}

func (s *Controller) getTeamId(r *http.Request) (uint, error) {
	user, ok := s.sessions.GetUser(r)
	if !ok {
		return 0, errors.New("user not found")
	}

	teamId, err := s.storageServices.Database.GetTeamId(user.ID)
	if err != nil {
		return 0, err
	}

	return teamId, nil
}
