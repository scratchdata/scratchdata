package connections

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/schema"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view"
	"github.com/scratchdata/scratchdata/pkg/view/model"
)

type Service struct {
	c               config.DashboardConfig
	gv              *goview.ViewEngine
	modelLoader     *model.ModelLoader
	sessions        *view.Service
	storageServices *storage.Services
	formDecoder     *schema.Decoder
	destManager     *destinations.DestinationManager
}

type Middleware func(http.Handler) http.Handler

func NewService() *Service {
	return &Service{}
}

type ConnRequestRequest struct {
	Type   string
	TeamId uint
}

type ConnRequestResponse struct {
	RequestId string
}

func (s *Service) ConnRequest(ctx context.Context, r *ConnRequestRequest) (*ConnRequestResponse, error) {
	_, ok := destinations.ViewConfig[r.Type]
	if !ok {
		return nil, fmt.Errorf("unknown type: %s", r.Type)
	}

	// TODO breachris name comes from form
	name := fmt.Sprintf("%s Request", r.Type)

	dest, err := s.storageServices.Database.CreateDestination(
		ctx, r.TeamId, name, r.Type, map[string]any{},
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
		RequestId: req.RequestID,
	}, nil
}

type HomeRequest struct {
	TeamId uint
}

type HomeResponse struct {
	Dests []config.Destination
}

func (s *Service) Home(ctx context.Context, r *HomeRequest) (*HomeResponse, error) {
	destModels, err := s.storageServices.Database.GetDestinations(ctx, r.TeamId)
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

func NewRouter(s *Service) *chi.Mux {
	connRouter := chi.NewRouter()

	connRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		res, err := s.Home(r.Context(), &HomeRequest{
			TeamId: teamId,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.Connections = view.Connections{
			Destinations: res.Dests,
		}
		err = s.gv.Render(w, http.StatusOK, "pages/connections/index", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Get("/new", func(w http.ResponseWriter, r *http.Request) {
		err := s.gv.Render(w, http.StatusOK, "pages/connections/new", s.modelLoader.Load(r, w))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/request", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		res, err := s.ConnRequest(r.Context(), &ConnRequestRequest{
			Type:   r.Form.Get("type"),
			TeamId: teamId,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.Request = view.Request{
			URL: fmt.Sprintf(
				"%s/dashboard/request/%s",
				s.c.ExternalURL,
				res.RequestId,
			),
		}

		err = s.gv.Render(w, http.StatusOK, "pages/request/link", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/upsert", func(w http.ResponseWriter, r *http.Request) {
		_, err := s.sessions.GetFlashes(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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

		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		req := &ConnUpsertRequest{
			RequireRequestID: false,
			Name:             r.Form.Get("name"),
			Type:             r.Form.Get("type"),
			RequestId:        r.Form.Get("request_id"),
			TeamId:           teamId,
			PostForm:         r.PostForm,
		}

		res, err := s.ConnUpsert(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := s.modelLoader.Load(r, w)

		m.Connect.APIKey = res.APIKey
		m.Connect.APIUrl = s.c.ExternalURL

		err = s.gv.Render(w, http.StatusOK, "pages/connections/api", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/keys", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
		}

		destIDStr := r.Form.Get("id")
		if destIDStr == "" {
			http.Error(w, "Destination ID required", http.StatusBadRequest)
			return
		}

		destID, err := strconv.ParseUint(destIDStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		dest, err := s.storageServices.Database.GetDestination(r.Context(), teamId, uint(destID))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		key := uuid.New().String()
		hashedKey := s.storageServices.Database.Hash(key)
		err = s.storageServices.Database.AddAPIKey(r.Context(), int64(dest.ID), hashedKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.Connect.APIKey = key
		m.Connect.APIUrl = s.c.ExternalURL
		err = s.gv.Render(w, http.StatusOK, "pages/connections/api", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Get("/new/{type}", func(w http.ResponseWriter, r *http.Request) {
		t := chi.URLParam(r, "type")
		if t == "" {
			http.Error(w, "No connection type specified", http.StatusBadRequest)
			return
		}

		vc, ok := destinations.ViewConfig[t]
		if !ok {
			http.Error(w, "Unknown connection type", http.StatusBadRequest)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.UpsertConnection = view.UpsertConnection{
			Destination: config.Destination{
				ID:   0,
				Type: t,
			},
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		s.renderNewConnection(w, r, m)
	})

	connRouter.Get("/edit/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, ok := s.sessions.GetUser(r)
		if !ok {
			http.Error(w, "User not found", http.StatusInternalServerError)
			return
		}

		dests, err := s.storageServices.Database.GetDestinations(r.Context(), user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var dest models.Destination
		for _, d := range dests {
			if d.ID == uint(id) {
				dest = d
				break
			}
		}

		if dest.ID == 0 {
			http.Error(w, "Destination not found", http.StatusNotFound)
			return
		}

		vc, ok := destinations.ViewConfig[dest.Type]
		if !ok {
			http.Error(w, "Unknown connection type", http.StatusBadRequest)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.UpsertConnection = view.UpsertConnection{
			Destination: dest.ToConfig(),
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		err = s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/delete", func(w http.ResponseWriter, r *http.Request) {
		idStr := r.Form.Get("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = s.storageServices.Database.GetDestination(r.Context(), teamId, uint(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = s.storageServices.Database.DeleteDestination(r.Context(), teamId, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/dashboard/connections", http.StatusFound)
	})

	reqRouter := chi.NewRouter()

	reqRouter.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
		requestId := chi.URLParam(r, "id")

		if requestId == "" {
			http.Error(w, "ConnRequest ID required", http.StatusBadRequest)
			return
		}
		connReq, err := s.validateRequestId(r.Context(), requestId)
		if err != nil {
			s.sessions.NewFlash(w, r, view.Flash{
				Type:  view.FlashTypeError,
				Title: err.Error(),
			})
			return
		}

		vc, ok := destinations.ViewConfig[connReq.Destination.Type]
		if !ok {
			http.Error(w, "Unknown connection type", http.StatusBadRequest)
			return
		}

		m := s.modelLoader.Load(r, w)
		m.UpsertConnection = view.UpsertConnection{
			Destination: connReq.Destination.ToConfig(),
			RequestID:   requestId,
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		m.HideSidebar = true

		err = s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	reqRouter.Post("/upsert", func(w http.ResponseWriter, r *http.Request) {
		upsertConn(w, r, true)
	})

	reqRouter.Get("/success", func(w http.ResponseWriter, r *http.Request) {
		m := s.modelLoader.Load(r, w)
		m.HideSidebar = true
		err := s.gv.Render(w, http.StatusOK, "pages/request/success", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return connRouter
}

func (s *Service) validateRequestId(c context.Context, requestId string) (models.ConnectionRequest, error) {
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
