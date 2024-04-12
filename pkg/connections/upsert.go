package connections

import (
	"context"
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

type ConnUpsertRequest struct {
	Name             string
	Type             string
	RequestId        string
	RequireRequestID bool
	PostForm         map[string][]string
	// TODO breadchris move to middleware
	TeamId uint
}

type FormError struct {
	Title   string
	Message string
}

type ConnUpsertResponse struct {
	Errors   []FormError
	Settings map[string]any
	APIKey   string
}

func singleFormError(title, message string) []FormError {
	return []FormError{
		{
			Title:   title,
			Message: message,
		},
	}
}

func (s *Service) ConnUpsert(ctx context.Context, req *ConnUpsertRequest) (*ConnUpsertResponse, error) {
	res := &ConnUpsertResponse{}
	for k, v := range req.PostForm {
		if len(v) == 1 {
			res.Settings[k] = v[0]
		}
	}

	var (
		err     error
		connReq models.ConnectionRequest
	)
	if req.RequireRequestID {
		connReq, err = s.validateRequestId(ctx, req.RequestId)
		if err != nil {
			res.Errors = singleFormError("Request ID not provided", err.Error())
			return res, nil
		}
	}

	vc, ok := destinations.ViewConfig[req.Type]
	if !ok {
		res.Errors = singleFormError("Unknown connection type", err.Error())
		return res, nil
	}

	instance := reflect.New(reflect.TypeOf(vc.Type)).Interface()

	err = s.formDecoder.Decode(instance, req.PostForm)
	if err != nil {
		res.Errors = singleFormError("Failed to decode form", err.Error())
		return res, nil
	}

	var settings map[string]any
	err = mapstructure.Decode(instance, &settings)
	if err != nil {
		res.Errors = []FormError{
			{
				Title:   "Failed to decode form",
				Message: err.Error(),
			},
		}
		return res, nil
	}

	cd := config.Destination{
		Type:     req.Type,
		Name:     req.Name,
		Settings: settings,
	}

	err = s.destManager.TestCredentials(cd)
	if err != nil {
		log.Err(err).Msg("failed to connect to destination")
		res.Errors = singleFormError("Failed to connect to destination. Check the settings and try again.", err.Error())
		return res, nil
	}

	var teamId uint
	if connReq.ID == 0 {
		teamId = req.TeamId
	} else {
		teamId = connReq.Destination.TeamID
	}

	if connReq.ID != 0 {
		connReq.Destination.Name = req.Name
		connReq.Destination.Settings = datatypes.NewJSONType(settings)

		err = s.storageServices.Database.UpdateDestination(ctx, connReq.Destination)
		if err != nil {
			res.Errors = singleFormError("Failed to update destination", err.Error())
			return res, nil
		}

		err = s.storageServices.Database.DeleteConnectionRequest(ctx, connReq.ID)
		if err != nil {
			log.Err(err).Msg("failed to delete connection request")
		}

		http.Redirect(w, req, "/dashboard/request/success", http.StatusFound)
		return
	}

	dest, err := s.storageServices.Database.CreateDestination(
		ctx, teamId, req.Name, req.Type, settings,
	)
	if err != nil {
		res.Errors = singleFormError("Failed to create destination", err.Error())
		return res, nil
	}

	res.APIKey = uuid.New().String()
	hashedKey := s.storageServices.Database.Hash(res.APIKey)
	err = s.storageServices.Database.AddAPIKey(ctx, int64(dest.ID), hashedKey)
	if err != nil {
		res.Errors = singleFormError("Failed to create destination", err.Error())
		return res, nil
	}
	return res, nil
}

func (s *Service) renderNewConnection(w http.ResponseWriter, r *http.Request, m view.Model) {
	err := s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Service) flashAndRender(w http.ResponseWriter, r *http.Request, f view.Flash, form FormState) {
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

func (s *Service) getTeamId(r *http.Request) (uint, error) {
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
