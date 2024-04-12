package view

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/connections"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view/model"
)

type FormState struct {
	Name        string
	Type        string
	Settings    map[string]any
	RequestID   string
	HideSidebar bool
}

type Controller struct {
	sessions    *SessionService
	conns       *connections.Service
	modelLoader *model.ModelLoader
	gv          *goview.ViewEngine
}

type Middleware func(http.Handler) http.Handler

func NewController(
	sessions *SessionService,
	conns *connections.Service,
	modelLoader *model.ModelLoader,
	gv *goview.ViewEngine,
) *Controller {
	return &Controller{
		sessions:    sessions,
		conns:       conns,
		modelLoader: modelLoader,
		gv:          gv,
	}
}

func (s *Controller) NewHomeRouter(middleware ...Middleware) *chi.Mux {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/", s.GetHome)
	return r
}

func (s *Controller) GetHome(w http.ResponseWriter, r *http.Request) {
	err := s.gv.Render(w, http.StatusOK, "pages/index", s.modelLoader.Load(r, w))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) NewConnRouter(middleware ...Middleware) *chi.Mux {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/", s.GetConnHome)
	r.Get("/new", s.GetNewConn)
	r.Get("/new/{type}", s.GetNewConnType)
	r.Post("/upsert", s.UpsertConn)
	r.Post("/request", s.NewConnRequest)
	r.Post("/keys", s.NewKey)
	r.Get("/edit/{id}", s.EditConn)
	r.Post("/delete", s.DeleteConn)
	return r
}

func (s *Controller) NewRequestRouter(middleware ...Middleware) *chi.Mux {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/{id}", s.GetRequest)
	r.Post("/upsert", s.UpsertRequest)
	r.Get("/success", s.GetRequestSuccess)
	return r
}

func (s *Controller) NewShareRouter(middleware ...Middleware) *chi.Mux {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/{uuid}", s.GetShare)
	r.Get("/{uuid}/download", s.GetShareDownload)
	r.Get("/share/{uuid}", func(w http.ResponseWriter, r *http.Request) {
		queryUUID := chi.URLParam(r, "uuid")

		id, err := uuid.Parse(queryUUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cachedQuery, found := storageServices.Database.GetShareQuery(r.Context(), id)
		if !found {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		year, month, day := cachedQuery.ExpiresAt.Date()

		m := Model{
			HideSidebar: true,
			ShareQuery: ShareQuery{
				Expires: fmt.Sprintf("%s %d, %d", month.String(), day, year),
				ID:      id.String(),
				Name:    cachedQuery.Name,
			},
		}
		if err := gv.Render(w, http.StatusOK, "pages/share", m); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/share/{uuid}/download", func(w http.ResponseWriter, r *http.Request) {
		format := strings.ToLower(r.URL.Query().Get("format"))

		queryUUID := chi.URLParam(r, "uuid")

		id, err := uuid.Parse(queryUUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cachedQuery, found := s.storageServices.Database.GetShareQuery(r.Context(), id)
		if !found {
			http.Error(w, "Query not found", http.StatusNotFound)
			return
		}

		dest, err := s.destinationManager.Destination(r.Context(), cachedQuery.DestinationID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		switch format {
		case "csv":
			w.Header().Set("Content-Type", "text/csv")
			if err := dest.QueryCSV(cachedQuery.Query, w); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		default:
			w.Header().Set("Content-Type", "application/json")
			if err := dest.QueryJSON(cachedQuery.Query, w); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})
	return r
}

func (s *Controller) GetConnHome(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.Home(r.Context(), &connections.HomeRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m := s.modelLoader.Load(r, w)
	m.Connections = Connections{
		Destinations: res.Dests,
	}
	err = s.gv.Render(w, http.StatusOK, "pages/connections/index", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) GetNewConn(w http.ResponseWriter, r *http.Request) {
	err := s.gv.Render(w, http.StatusOK, "pages/connections/new", s.modelLoader.Load(r, w))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) NewConnRequest(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.ConnRequest(r.Context(), &connections.ConnRequestRequest{
		Type: r.Form.Get("type"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m := s.modelLoader.Load(r, w)
	m.Request = Request{
		URL: res.URL,
	}

	err = s.gv.Render(w, http.StatusOK, "pages/request/link", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) UpsertConn(w http.ResponseWriter, r *http.Request) {
	req, err := s.upsertRequestFromForm(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.conns.ConnUpsert(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m := s.modelLoader.Load(r, w)
	m.Connect.APIKey = res.APIKey

	err = s.gv.Render(w, http.StatusOK, "pages/connections/api", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) NewKey(w http.ResponseWriter, r *http.Request) {
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

	res, err := s.conns.NewKey(r.Context(), &connections.NewKeyRequest{
		DestID: uint(destID),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m := s.modelLoader.Load(r, w)
	m.Connect.APIKey = res.APIKey
	m.Connect.APIUrl = res.ExternalURL
	err = s.gv.Render(w, http.StatusOK, "pages/connections/api", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) GetNewConnType(w http.ResponseWriter, r *http.Request) {
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
	m.UpsertConnection = UpsertConnection{
		Destination: config.Destination{
			ID:   0,
			Type: t,
		},
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
	}
	s.renderNewConnection(w, r, m)
}

func (s *Controller) EditConn(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.conns.GetDestination(r.Context(), &connections.GetDestinationRequest{
		DestID: uint(id),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	m := s.modelLoader.Load(r, w)
	m.UpsertConnection = UpsertConnection{
		Destination: res.Destination,
		TypeDisplay: res.TypeDisplay,
		FormFields:  res.FormFields,
	}
	err = s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) DeleteConn(w http.ResponseWriter, r *http.Request) {
	idStr := r.Form.Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = s.conns.DeleteDestination(r.Context(), &connections.DeleteDestinationRequest{
		DestID: uint(id),
	})

	http.Redirect(w, r, "/dashboard/connections", http.StatusFound)
}

func (s *Controller) GetRequest(w http.ResponseWriter, r *http.Request) {
	requestId := chi.URLParam(r, "id")

	if requestId == "" {
		http.Error(w, "ConnRequest ID required", http.StatusBadRequest)
		return
	}
	connReq, err := s.conns.ValidateRequestId(r.Context(), requestId)
	if err != nil {
		s.sessions.NewFlash(w, r, Flash{
			Type:  FlashTypeError,
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
	m.UpsertConnection = UpsertConnection{
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
}

func (s *Controller) UpsertRequest(w http.ResponseWriter, r *http.Request) {
	req, err := s.upsertRequestFromForm(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.conns.UpdateConnection(r.Context(), &connections.UpdateConnectionRequest{
		RequestID: req.RequestId,
		Req:       req,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(res.Errors) > 0 {
		s.flashAndRenderUpsertConn(w, r, Flash{
			Type:    FlashTypeError,
			Title:   res.Errors[0].Title,
			Message: res.Errors[0].Message,
		}, FormState{
			Name:      req.Name,
			Type:      req.Type,
			Settings:  res.Settings,
			RequestID: req.RequestId,
		})
		return

	}

	http.Redirect(w, r, "/dashboard/request/success", http.StatusFound)
}

func (s *Controller) GetRequestSuccess(w http.ResponseWriter, r *http.Request) {
	m := s.modelLoader.Load(r, w)
	m.HideSidebar = true
	err := s.gv.Render(w, http.StatusOK, "pages/request/success", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) renderNewConnection(w http.ResponseWriter, r *http.Request, m Model) {
	err := s.gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Controller) flashAndRenderUpsertConn(w http.ResponseWriter, r *http.Request, f Flash, form FormState) {
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

	m.UpsertConnection = UpsertConnection{
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

func (s *Controller) upsertRequestFromForm(w http.ResponseWriter, r *http.Request) (*connections.ConnUpsertRequest, error) {
	_, err := s.sessions.GetFlashes(w, r)
	if err != nil {
		return nil, err
	}

	err = r.ParseForm()
	if err != nil {
		return nil, err
	}

	settings := map[string]any{}
	for k, v := range r.PostForm {
		settings[k] = v[0]
	}

	return &connections.ConnUpsertRequest{
		Name:      r.Form.Get("name"),
		Type:      r.Form.Get("type"),
		RequestId: r.Form.Get("request_id"),
		PostForm:  r.PostForm,
	}, nil
}
