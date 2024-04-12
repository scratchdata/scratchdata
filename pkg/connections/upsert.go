package connections

import (
	"context"
	"errors"
	"reflect"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/view"
)

type ConnUpsertRequest struct {
	Name      string
	Type      string
	RequestId string
	PostForm  map[string][]string
	// TODO breadchris move to middleware
	TeamId uint
}

type FormError struct {
	Title   string
	Message string
}

type ConnUpsertResponse struct {
	Errors      []FormError
	Settings    map[string]any
	APIKey      string
	ExternalURL string
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
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	res := &ConnUpsertResponse{
		ExternalURL: s.c.ExternalURL,
	}
	for k, v := range req.PostForm {
		if len(v) == 1 {
			res.Settings[k] = v[0]
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

func (s *Service) getTeamId(ctx context.Context) (uint, error) {
	user, ok := view.GetUser(ctx)
	if !ok {
		return 0, errors.New("user not found")
	}

	teamId, err := s.storageServices.Database.GetTeamId(user.ID)
	if err != nil {
		return 0, err
	}

	return teamId, nil
}
