package view

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/connections"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view/session"
)

type Controller struct {
	session *session.Service
	conns   *connections.Service
	view    *View
}

type Middleware func(http.Handler) http.Handler

func NewController(
	sessions *session.Service,
	conns *connections.Service,
	v *View,
) *Controller {
	return &Controller{
		session: sessions,
		conns:   conns,
		view:    v,
	}
}

func (s *Controller) HomeRoute(middleware ...Middleware) chi.Router {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/", s.GetHome)
	return r
}

func (s *Controller) GetHome(w http.ResponseWriter, r *http.Request) {
	s.view.Render(w, r, http.StatusOK, "pages/index", nil)
}

func (s *Controller) ConnRoutes(middleware ...Middleware) chi.Router {
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

func (s *Controller) RequestRoutes(middleware ...Middleware) chi.Router {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/{id}", s.GetRequest)
	r.Post("/upsert", s.UpsertRequest)
	r.Get("/success", s.GetRequestSuccess)
	return r
}

func (s *Controller) QueryRoutes(middleware ...Middleware) chi.Router {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/", s.GetQueryHome)
	r.Get("/upsert", s.GetUpsertQuery)
	r.Post("/upsert", s.UpsertNewQuery)
	r.Post("/delete", s.DeleteQuery)
	return r
}

func (s *Controller) KeyRoutes(middleware ...Middleware) chi.Router {
	r := chi.NewRouter()
	for _, m := range middleware {
		r.Use(m)
	}
	r.Get("/", s.GetKeyHome)
	r.Get("/upsert", s.GetUpsertKey)
	r.Post("/upsert", s.UpsertNewKey)
	r.Post("/delete", s.DeleteKey)
	return r
}

func (s *Controller) GetKeyHome(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.GetKeys(r.Context(), &connections.GetKeysRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/key/index", res)
}

func (s *Controller) GetUpsertKey(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		id = 0
	}

	res, err := s.conns.NewKey(r.Context(), &connections.NewKeyRequest{
		ID: uint(id),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/key/upsert", res)
}

func (s *Controller) UpsertNewKey(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	name := r.Form.Get("name")
	if name == "" {
		http.Error(w, "Name cannot be empty", http.StatusBadRequest)
		return
	}

	queryIDStr := r.Form.Get("query_id")
	queryID, err := strconv.ParseUint(queryIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Query ID required", http.StatusBadRequest)
		return
	}

	keyIDStr := r.Form.Get("id")

	var keyID uint64
	if keyIDStr != "" {
		keyID, err = strconv.ParseUint(keyIDStr, 10, 64)
		if err != nil {
			http.Error(w, "Key ID required", http.StatusBadRequest)
			return
		}
	}

	res, err := s.conns.UpsertKey(r.Context(), &connections.UpsertKeyRequest{
		ID:      uint(keyID),
		QueryID: uint(queryID),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/key/success", res)
}

func (s *Controller) DeleteKey(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	idStr := r.Form.Get("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = s.conns.DeleteKey(r.Context(), &connections.DeleteKeyRequest{
		ID: uint(id),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/dashboard/key", http.StatusFound)
}

func (s *Controller) GetQueryHome(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.GetQueries(r.Context(), &connections.GetQueriesRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/query/index", res)
}

func (s *Controller) GetUpsertQuery(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		id = 0
	}

	res, err := s.conns.NewQuery(r.Context(), &connections.NewQueryRequest{
		ID: uint(id),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/query/upsert", res)
}

func (s *Controller) UpsertNewQuery(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := r.Form.Get("query")
	if query == "" {
		http.Error(w, "Query cannot be empty", http.StatusBadRequest)
		return
	}

	name := r.Form.Get("name")
	if name == "" {
		http.Error(w, "Name cannot be empty", http.StatusBadRequest)
		return
	}

	public := r.Form.Get("public") == "on"

	destIDStr := r.Form.Get("dest_id")
	destID, err := strconv.ParseUint(destIDStr, 10, 64)
	if err != nil {
		http.Error(w, "Destination ID required", http.StatusBadRequest)
		return
	}

	queryIDStr := r.Form.Get("id")

	var queryID uint64
	if queryIDStr != "" {
		queryID, err = strconv.ParseUint(queryIDStr, 10, 64)
		if err != nil {
			http.Error(w, "Destination ID required", http.StatusBadRequest)
			return
		}
	}

	res, err := s.conns.UpsertQuery(r.Context(), &connections.UpsertQueryRequest{
		ID:     uint(queryID),
		DestID: uint(destID),
		Query:  query,
		Name:   name,
		Public: public,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if res.URL != "" {
		s.view.Render(w, r, http.StatusOK, "pages/query/success", res)
	} else {
		http.Redirect(w, r, "/dashboard/query", http.StatusFound)
	}
}

func (s *Controller) DeleteQuery(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	idStr := r.Form.Get("id")
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = s.conns.DeleteQuery(r.Context(), &connections.DeleteQueryRequest{
		ID: uint(id),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/dashboard/query", http.StatusFound)
}

func (s *Controller) GetConnHome(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.Home(r.Context(), &connections.HomeRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/connections/index", res)
}

func (s *Controller) GetNewConn(w http.ResponseWriter, r *http.Request) {
	s.view.Render(w, r, http.StatusOK, "pages/connections/new", nil)
}

func (s *Controller) NewConnRequest(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.ConnRequest(r.Context(), &connections.ConnRequestRequest{
		Type: r.Form.Get("type"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/request/link", res)
}

func (s *Controller) UpsertConn(w http.ResponseWriter, r *http.Request) {
	req, err := s.upsertRequestFromForm(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res, err := s.conns.ConnUpsert(r.Context(), req)
	if err != nil {
		var fe connections.FormError
		if errors.As(err, &fe) {
			s.flashAndRenderUpsertConn(w, r, fe)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/connections/api", res)
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

	res, err := s.conns.NewDestinationKey(r.Context(), &connections.NewDestinationKeyRequest{
		DestID: uint(destID),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/connections/api", res)
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

	res := connections.GetDestinationResponse{
		Destination: config.Destination{
			ID:   0,
			Type: t,
		},
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
	}
	s.view.Render(w, r, http.StatusOK, "pages/connections/upsert", res)
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
	s.view.Render(w, r, http.StatusOK, "pages/connections/upsert", res)
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
		s.session.NewFlash(w, r, session.Flash{
			Type:  session.FlashTypeError,
			Title: err.Error(),
		})
		return
	}

	vc, ok := destinations.ViewConfig[connReq.Destination.Type]
	if !ok {
		http.Error(w, "Unknown connection type", http.StatusBadRequest)
		return
	}

	res := connections.GetDestinationResponse{
		Destination: connReq.Destination.ToConfig(),
		RequestID:   requestId,
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
	}
	s.view.RenderExternal(w, r, http.StatusOK, "pages/connections/upsert", res)
}

func (s *Controller) UpsertRequest(w http.ResponseWriter, r *http.Request) {
	req, err := s.upsertRequestFromForm(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = s.conns.UpdateConnection(r.Context(), &connections.UpdateConnectionRequest{
		RequestID: req.RequestId,
		Req:       req,
	})
	if err != nil {
		var fe connections.FormError
		if errors.As(err, &fe) {
			s.flashAndRenderUpsertConn(w, r, fe)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/request/success", http.StatusFound)
}

func (s *Controller) GetRequestSuccess(w http.ResponseWriter, r *http.Request) {
	s.view.RenderExternal(w, r, http.StatusOK, "pages/request/success", nil)
}

func (s *Controller) flashAndRenderUpsertConn(
	w http.ResponseWriter,
	r *http.Request,
	fe connections.FormError,
) {
	f := session.Flash{
		Type:    session.FlashTypeError,
		Title:   fe.Title,
		Message: fe.Message,
	}
	log.Info().Interface("flash", f).Msg("failed to create destination")
	s.session.NewFlash(w, r, f)

	if f.Fatal {
		s.view.Render(w, r, http.StatusInternalServerError, "pages/connections/fatal", nil)
		return
	}

	vc, ok := destinations.ViewConfig[fe.State.Type]
	if !ok {
		http.Error(w, "Unknown connection type", http.StatusBadRequest)
		return
	}

	res := connections.GetDestinationResponse{
		Destination: config.Destination{
			ID:       0,
			Name:     fe.State.Name,
			Type:     fe.State.Type,
			Settings: fe.State.Settings,
		},
		TypeDisplay: vc.Display,
		FormFields:  util.ConvertToForms(vc.Type),
		RequestID:   fe.State.RequestID,
	}
	if res.RequestID == "" {
		s.view.Render(w, r, http.StatusOK, "pages/connections/upsert", res)
	} else {
		s.view.RenderExternal(w, r, http.StatusOK, "pages/connections/upsert", res)
	}
}

func (s *Controller) upsertRequestFromForm(w http.ResponseWriter, r *http.Request) (*connections.ConnUpsertRequest, error) {
	_, err := s.session.GetFlashes(w, r)
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
