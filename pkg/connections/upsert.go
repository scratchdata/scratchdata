package connections

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/view/session"
)

type ConnUpsertRequest struct {
	Name      string
	Type      string
	RequestId string
	PostForm  map[string][]string
	// TODO breadchris move to middleware
	TeamID uint
}

type FormState struct {
	Name      string
	Type      string
	Settings  map[string]any
	RequestID string
}

type FormError struct {
	Title   string
	Message string
	State   *ConnUpsertResponse
}

func (s FormError) Error() string {
	return fmt.Sprintf("%s: %s", s.Title, s.Message)
}

type ConnUpsertResponse struct {
	Name      string
	Type      string
	Settings  map[string]any
	APIKey    string
	APIURL    string
	RequestID string
}

func NewFormError(title, message string, res *ConnUpsertResponse) FormError {
	return FormError{
		Title:   title,
		Message: message,
		State:   res,
	}
}

func (s *Service) ConnUpsert(ctx context.Context, req *ConnUpsertRequest) (*ConnUpsertResponse, error) {
	var err error
	if req.TeamID == 0 {
		req.TeamID, err = s.getTeamId(ctx)
		if err != nil {
			return nil, err
		}
	}

	res := &ConnUpsertResponse{
		Name:      req.Name,
		Type:      req.Type,
		Settings:  map[string]any{},
		APIURL:    s.c.ExternalURL,
		RequestID: req.RequestId,
	}
	for k, v := range req.PostForm {
		if len(v) == 1 {
			res.Settings[k] = v[0]
		}
	}

	vc, ok := destinations.ViewConfig[req.Type]
	if !ok {
		return nil, NewFormError("Unknown connection type", req.Type, res)
	}

	instance := reflect.New(reflect.TypeOf(vc.Type)).Interface()

	err = s.formDecoder.Decode(instance, req.PostForm)
	if err != nil {
		return nil, NewFormError("Failed to decode form", err.Error(), res)
	}

	var settings map[string]any
	err = mapstructure.Decode(instance, &settings)
	if err != nil {
		return nil, NewFormError("Failed to decode form", err.Error(), res)
	}

	cd := config.Destination{
		Type:     req.Type,
		Name:     req.Name,
		Settings: settings,
	}

	err = s.destManager.TestCredentials(cd)
	if err != nil {
		log.Err(err).Msg("failed to connect to destination")
		return nil, NewFormError("Failed to connect to destination. Check the settings and try again.", err.Error(), res)
	}

	dest, err := s.storageServices.Database.CreateDestination(
		ctx, req.TeamID, req.Name, req.Type, settings,
	)
	if err != nil {
		return nil, NewFormError("Failed to create destination", err.Error(), res)
	}

	res.APIKey = uuid.New().String()
	err = s.storageServices.Database.AddAPIKey(ctx, int64(dest.ID), res.APIKey)
	if err != nil {
		return nil, NewFormError("Failed to create destination", err.Error(), res)
	}
	return res, nil
}

func (s *Service) getTeamId(ctx context.Context) (uint, error) {
	user, ok := session.GetUser(ctx)
	if !ok {
		return 0, errors.New("user not found")
	}

	teamId, err := s.storageServices.Database.GetTeamId(user.ID)
	if err != nil {
		return 0, err
	}

	return teamId, nil
}
