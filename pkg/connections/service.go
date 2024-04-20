package connections

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/schema"
	"github.com/gosimple/slug"
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
			"%s/request/%s",
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

type NewDestinationKeyRequest struct {
	DestID uint
}

type NewDestinationKeyResponse struct {
	APIKey string
	APIURL string
}

func (s *Service) NewDestinationKey(ctx context.Context, r *NewDestinationKeyRequest) (*NewDestinationKeyResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	dest, err := s.storageServices.Database.GetDestination(ctx, teamId, r.DestID)
	if err != nil {
		return nil, err
	}

	key := uuid.New().String()
	err = s.storageServices.Database.AddAPIKey(ctx, int64(dest.ID), key)
	if err != nil {
		return nil, err
	}
	return &NewDestinationKeyResponse{
		APIKey: key,
		APIURL: s.c.ExternalURL,
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

	r.Req.TeamID = connReq.Destination.TeamID
	res, err := s.ConnUpsert(ctx, r.Req)
	if err != nil {
		return nil, err
	}

	connReq.Destination.Name = r.Req.Name
	connReq.Destination.Settings = datatypes.NewJSONType(res.Settings)

	err = s.storageServices.Database.UpdateDestination(ctx, connReq.Destination)
	if err != nil {
		return nil, NewFormError("Failed to update destination", err.Error(), res)
	}

	err = s.storageServices.Database.DeleteConnectionRequest(ctx, connReq.ID)
	if err != nil {
		// TODO breadchris non-fatal error?
		log.Err(err).Msg("failed to delete connection request")
	}
	return res, nil
}

type GetQueriesRequest struct {
}

type Query struct {
	ID       uint
	Name     string
	Method   string
	Endpoint string
	Database string
}

type GetQueriesResponse struct {
	Queries []Query
}

func (s *Service) GetQueries(ctx context.Context, r *GetQueriesRequest) (*GetQueriesResponse, error) {
	teamID, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	queries := s.storageServices.Database.GetSavedQueries(ctx, teamID)

	res := &GetQueriesResponse{}
	for _, q := range queries {
		res.Queries = append(res.Queries, Query{
			ID:   q.ID,
			Name: q.Name,
			// TODO breadchris method
			Method:   "GET",
			Endpoint: fmt.Sprintf("/api/query/%s", q.Slug),
			Database: q.Destination.Name,
		})
	}
	return res, nil
}

type QueryParam struct {
	Name         string
	Type         string
	ExampleValue string
	Description  string
}

type FieldType struct {
	Value string
	Name  string
}

type NewQueryRequest struct {
	ID uint
}

type NewQueryResponse struct {
	SavedQuery   models.SavedQuery
	Destinations []models.Destination
}

func (s *Service) NewQuery(ctx context.Context, r *NewQueryRequest) (*NewQueryResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	var q models.SavedQuery
	if r.ID != 0 {
		q, err = s.storageServices.Database.GetSavedQueryByID(ctx, teamId, r.ID)
		if err != nil {
			return nil, err
		}
	}

	dests, err := s.storageServices.Database.GetDestinations(ctx, teamId)
	if err != nil {
		return nil, err
	}

	res := &NewQueryResponse{
		SavedQuery:   q,
		Destinations: dests,
	}
	// TODO breadchris move this to view
	if q.ID == 0 {
		res.SavedQuery.Query = "SELECT * FROM events WHERE user = $user"
	}

	return res, nil
}

type UpsertQueryRequest struct {
	ID     uint
	DestID uint
	Name   string
	Query  string
	Public bool
}

type UpsertQueryResponse struct {
	URL string
}

func (s *Service) UpsertQuery(ctx context.Context, r *UpsertQueryRequest) (*UpsertQueryResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	// check if the authenticated team has access to the destination
	_, err = s.storageServices.Database.GetDestination(ctx, teamId, r.DestID)
	if err != nil {
		return nil, err
	}

	var (
		querySlug string
		sq        models.SavedQuery
	)
	if r.ID != 0 {
		sq, err = s.storageServices.Database.GetSavedQueryByID(ctx, teamId, r.ID)
		if err != nil {
			return nil, err
		}
		sq.Name = r.Name
		sq.Query = r.Query
		sq.IsPublic = r.Public
	} else {
		querySlug = slug.Make(r.Name)
		_, ok := s.storageServices.Database.GetSavedQuery(ctx, teamId, querySlug)
		if ok {
			return nil, errors.New("query name already exists")
		}
		sq = models.NewSavedQuery(
			teamId,
			r.DestID,
			r.Name,
			r.Query,
			0,
			r.Public,
			querySlug,
		)
	}

	q, err := s.storageServices.Database.UpsertSavedQuery(ctx, sq)
	if err != nil {
		return nil, err
	}

	if r.ID == 0 {
		key := uuid.New().String()
		err = s.storageServices.Database.CreateSavedQueryAPIKey(ctx, q.ID, r.DestID, key, datatypes.JSONMap{}, teamId)
		if err != nil {
			return nil, err
		}

		return &UpsertQueryResponse{
			URL: fmt.Sprintf(
				"%s/api/query/%s?api_key=%s",
				s.c.ExternalURL,
				sq.Slug,
				key,
			),
		}, nil
	}
	return &UpsertQueryResponse{}, nil
}

type DeleteQueryRequest struct {
	ID uint
}

type DeleteQueryResponse struct{}

func (s *Service) DeleteQuery(ctx context.Context, r *DeleteQueryRequest) (*DeleteQueryResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	err = s.storageServices.Database.DeleteSavedQuery(ctx, teamId, r.ID)
	if err != nil {
		return nil, err
	}
	return &DeleteQueryResponse{}, nil
}

type SavedQueryKey struct {
	ID   uint
	Name string
}

type GetKeysRequest struct {
}

type GetKeysResponse struct {
	Keys []SavedQueryKey
}

func (s *Service) GetKeys(ctx context.Context, r *GetKeysRequest) (*GetKeysResponse, error) {
	teamID, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	keys, err := s.storageServices.Database.GetSavedQueryKeys(ctx, teamID)
	if err != nil {
		return nil, err
	}

	res := &GetKeysResponse{}
	for _, k := range keys {
		res.Keys = append(res.Keys, SavedQueryKey{
			ID:   k.ID,
			Name: fmt.Sprintf("For %s", k.SavedQuery.Name),
		})
	}
	return res, nil
}

type NewKeyRequest struct {
	ID uint
}

type NewKeyResponse struct {
}

func (s *Service) NewKey(ctx context.Context, r *NewKeyRequest) (*NewKeyResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	var q models.SavedQuery
	if r.ID != 0 {
		q, err = s.storageServices.Database.GetSavedQueryByID(ctx, teamId, r.ID)
		if err != nil {
			return nil, err
		}
	}

	res := &NewKeyResponse{}
	// TODO breadchris move this to view
	if q.ID == 0 {
	}

	return res, nil
}

type UpsertKeyRequest struct {
	ID uint
}

type UpsertKeyResponse struct {
	URL string
}

func (s *Service) UpsertKey(ctx context.Context, r *UpsertKeyRequest) (*UpsertKeyResponse, error) {
	//teamId, err := s.getTeamId(ctx)
	//if err != nil {
	//	return nil, err
	//}

	// check if the authenticated team has access to the destination
	//_, err = s.storageServices.Database.GetDestination(ctx, teamId, r.DestID)
	//if err != nil {
	//	return nil, err
	//}

	//var (
	//	sq        models.SavedQuery
	//)
	if r.ID != 0 {
		//sq, err = s.storageServices.Database.GetSavedQueryByID(ctx, teamId, r.ID)
		//if err != nil {
		//	return nil, err
		//}
	} else {
		//querySlug = slug.Make(r.Name)
		//_, ok := s.storageServices.Database.GetSavedQuery(ctx, teamId, querySlug)
		//if ok {
		//	return nil, errors.New("query name already exists")
		//}
		//sq = models.NewSavedQuery(
		//	teamId,
		//	r.DestID,
		//	r.Name,
		//	r.Query,
		//	0,
		//	r.Public,
		//	querySlug,
		//)
	}

	//q, err := s.storageServices.Database.UpsertSavedQuery(ctx, sq)
	//if err != nil {
	//	return nil, err
	//}

	if r.ID == 0 {
		//key := uuid.New().String()
		//err = s.storageServices.Database.CreateSavedQueryAPIKey(ctx, q.ID, r.DestID, key, datatypes.JSONMap{})
		//if err != nil {
		//	return nil, err
		//}

		return &UpsertKeyResponse{}, nil
	}
	return &UpsertKeyResponse{}, nil
}

type DeleteKeyRequest struct {
	ID uint
}

type DeleteKeyResponse struct{}

func (s *Service) DeleteKey(ctx context.Context, r *DeleteKeyRequest) (*DeleteKeyResponse, error) {
	teamId, err := s.getTeamId(ctx)
	if err != nil {
		return nil, err
	}

	err = s.storageServices.Database.DeleteSavedQuery(ctx, teamId, r.ID)
	if err != nil {
		return nil, err
	}
	return &DeleteKeyResponse{}, nil
}
