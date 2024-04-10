package request

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view"
	"github.com/scratchdata/scratchdata/pkg/view/connections"
	"github.com/scratchdata/scratchdata/pkg/view/model"
	"github.com/scratchdata/scratchdata/pkg/view/session"
)

type Controller struct {
	gv              *goview.ViewEngine
	storageServices *storage.Services
	modelLoader     *model.ModelLoader
	sessions        *session.Service
}

func NewController() *Controller {
	return &Controller{}
}

func (s *Controller) NewRouter(m ...connections.Middleware) *chi.Mux {
	reqRouter := chi.NewRouter()

	for _, mw := range m {
		reqRouter.Use(mw)
	}

	reqRouter.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
		requestId := chi.URLParam(r, "id")

		if requestId == "" {
			http.Error(w, "Request ID required", http.StatusBadRequest)
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
	return reqRouter
}

func (s *Controller) validateRequestId(c context.Context, requestId string) (models.ConnectionRequest, error) {
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
