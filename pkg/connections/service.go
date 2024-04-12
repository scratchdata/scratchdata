package connections

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/schema"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view/session"
	"gorm.io/datatypes"
)

type Service struct {
	c               config.DashboardConfig
	storageServices *storage.Services
	formDecoder     *schema.Decoder
	destManager     *destinations.DestinationManager
}

type Middleware func(http.Handler) http.Handler

func NewService(
	c config.DashboardConfig,
	storageServices *storage.Services,
	destManager *destinations.DestinationManager,
) *Service {
	formDecoder := schema.NewDecoder()
	formDecoder.IgnoreUnknownKeys(true)
	return &Service{
		c:               c,
		storageServices: storageServices,
		formDecoder:     formDecoder,
		destManager:     destManager,
	}
}

type ConnRequestRequest struct {
	Type string
}

type ConnRequestResponse struct {
	URL string
}

func (s *Service) ConnRequest(ctx context.Context, r *ConnRequestRequest) (*ConnRequestResponse, error) {
	teamID, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	_, ok := destinations.ViewConfig[r.Type]
	if !ok {
		return nil, fmt.Errorf("unknown type: %s", r.Type)
	}

	// TODO breachris name comes from form
	name := fmt.Sprintf("%s Request", r.Type)

	dest, err := s.storageServices.Database.CreateDestination(
		ctx, teamID, name, r.Type, map[string]any{},
	)
	if err != nil {
		return nil, err
	}

	req, err := s.storageServices.Database.CreateConnectionRequest(ctx, dest)
	if err != nil {
		return nil, err
	}

	if req.ID == 0 {
		return nil, err
	}
	return &ConnRequestResponse{
		URL: fmt.Sprintf(
			"%s/dashboard/request/%s",
			s.c.ExternalURL,
			req.RequestID,
		),
	}, nil
}

type HomeRequest struct {
}

type HomeResponse struct {
	Dests []config.Destination
}

func (s *Service) Home(ctx context.Context, r *HomeRequest) (*HomeResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	destModels, err := s.storageServices.Database.GetDestinations(ctx, teamId)
	if err != nil {
		return nil, err
	}

	dests := []config.Destination{}
	for _, d := range destModels {
		dests = append(dests, d.ToConfig())
	}
	return &HomeResponse{
		Dests: dests,
	}, nil
}

type NewKeyRequest struct {
	DestID uint
}

type NewKeyResponse struct {
	APIKey      string
	ExternalURL string
}

func (s *Service) NewKey(ctx context.Context, r *NewKeyRequest) (*NewKeyResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	dest, err := s.storageServices.Database.GetDestination(ctx, teamId, r.DestID)
	if err != nil {
		return nil, err
	}

	key := uuid.New().String()
	hashedKey := s.storageServices.Database.Hash(key)
	err = s.storageServices.Database.AddAPIKey(ctx, int64(dest.ID), hashedKey)
	if err != nil {
		return nil, err
	}
	return &NewKeyResponse{
		APIKey:      key,
		ExternalURL: s.c.ExternalURL,
	}, nil
}

type GetDestinationRequest struct {
	DestID uint
}

type GetDestinationResponse struct {
	Destination config.Destination
	TypeDisplay string
	FormFields  []util.Form
	RequestID   string
}

func (s *Service) GetDestination(ctx context.Context, r *GetDestinationRequest) (*GetDestinationResponse, error) {
	user, ok := session.GetUser(ctx)
	if !ok {
		return nil, errors.New("user not found")
	}

	dests, err := s.storageServices.Database.GetDestinations(ctx, user.ID)
	if err != nil {
		return nil, err
	}

	var dest models.Destination
	for _, d := range dests {
		if d.ID == r.DestID {
			dest = d
			break
		}
	}

	if dest.ID == 0 {
		return nil, errors.New("destination not found")
	}

	vc, ok := destinations.ViewConfig[dest.Type]
	if !ok {
		return nil, errors.New("unknown connection type")
	}
	return &GetDestinationResponse{
		Destination: dest.ToConfig(),
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
	}, nil
}

type DeleteDestinationRequest struct {
	DestID uint
}

type DeleteDestinationResponse struct{}

func (s *Service) DeleteDestination(ctx context.Context, r *DeleteDestinationRequest) (*DeleteDestinationResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	_, err = s.storageServices.Database.GetDestination(ctx, teamId, r.DestID)
	if err != nil {
		return nil, err
	}

	err = s.storageServices.Database.DeleteDestination(ctx, teamId, r.DestID)
	if err != nil {
		return nil, err
	}
	return &DeleteDestinationResponse{}, nil
}

func (s *Service) ValidateRequestId(c context.Context, requestId string) (models.ConnectionRequest, error) {
	_, err := uuid.Parse(requestId)
	if err != nil {
		return models.ConnectionRequest{}, errors.New("invalid request id")
	}

	req, err := s.storageServices.Database.GetConnectionRequest(c, uuid.MustParse(requestId))
	if err != nil {
		return models.ConnectionRequest{}, errors.New("failed to lookup request")
	}

	if req.Expiration.Before(time.Now()) {
		// TODO breadchris if the request is expired, suggest creating a new one
		return models.ConnectionRequest{}, errors.New("request expired")
	}
	return req, nil
}

type VerifyRequestRequest struct {
	RequestID string
}

type VerifyRequestResponse struct {
}

func (s *Service) VerifyRequest(ctx context.Context, r *VerifyRequestRequest) (*VerifyRequestResponse, error) {
	_, err := s.ValidateRequestId(ctx, r.RequestID)
	if err != nil {
		return nil, err
	}
	return &VerifyRequestResponse{}, nil
}

type UpdateConnectionRequest struct {
	RequestID string
	Req       *ConnUpsertRequest
}

type UpdateConnectionResponse struct {
}

func (s *Service) UpdateConnection(ctx context.Context, r *UpdateConnectionRequest) (*ConnUpsertResponse, error) {
	connReq, err := s.ValidateRequestId(ctx, r.RequestID)
	if err != nil {
		// TODO breadchris errors states should be flash messages
		return nil, err
	}

	res, err := s.ConnUpsert(ctx, r.Req)
	if err != nil {
		return nil, err
	}

	connReq.Destination.Name = r.Req.Name
	connReq.Destination.Settings = datatypes.NewJSONType(res.Settings)

	err = s.storageServices.Database.UpdateDestination(ctx, connReq.Destination)
	if err != nil {
		return nil, NewFormError("Failed to update destination", err.Error())
	}

	err = s.storageServices.Database.DeleteConnectionRequest(ctx, connReq.ID)
	if err != nil {
		// TODO breadchris non-fatal error?
		log.Err(err).Msg("failed to delete connection request")
	}
	return res, nil
}
