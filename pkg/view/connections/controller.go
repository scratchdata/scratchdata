package connections

import (
	"fmt"
	"net/http"
	"strconv"

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
	"github.com/scratchdata/scratchdata/pkg/view/session"
)

type Controller struct {
	c               config.DashboardConfig
	gv              *goview.ViewEngine
	modelLoader     *model.ModelLoader
	sessions        *session.Service
	storageServices *storage.Services
	formDecoder     *schema.Decoder
	destManager     *destinations.DestinationManager
}

type Middleware func(http.Handler) http.Handler

func NewController() *Controller {
	return &Controller{}
}

func (s *Controller) NewRouter(middleware ...Middleware) *chi.Mux {
	connRouter := chi.NewRouter()

	// TODO: Want to be able to disable this for quick local dev
	for _, m := range middleware {
		connRouter.Use(m)
	}
	//connRouter.Use(auth)
	//connRouter.Use(csrfMiddleware)

	connRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := s.getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		destModels, err := s.storageServices.Database.GetDestinations(r.Context(), teamId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		dests := []config.Destination{}
		for _, d := range destModels {
			dests = append(dests, d.ToConfig())
		}

		m := s.modelLoader.Load(r, w)
		m.Connections = view.Connections{
			Destinations: dests,
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
		}

		t := r.Form.Get("type")

		_, ok := destinations.ViewConfig[t]
		if !ok {
			http.Error(w, "Unknown connection type", http.StatusBadRequest)
			return
		}

		// TODO breachris name comes from form
		name := fmt.Sprintf("%s Request", t)

		dest, err := s.storageServices.Database.CreateDestination(
			r.Context(), teamId, name, t, map[string]any{},
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		req, err := s.storageServices.Database.CreateConnectionRequest(r.Context(), dest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if req.ID == 0 {
			http.Error(w, "Failed to create request", http.StatusInternalServerError)
			return
		}

		m := s.modelLoader.Load(r, w)

		m.Request = view.Request{
			URL: fmt.Sprintf(
				"%s/dashboard/request/%s",
				s.c.ExternalURL,
				req.RequestID,
			),
		}

		err = s.gv.Render(w, http.StatusOK, "pages/request/link", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/upsert", func(w http.ResponseWriter, r *http.Request) {
		s.upsertConn(w, r, false)
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
	return connRouter
}
