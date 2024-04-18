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
	r.Get("/new", s.GetQueryNew)
	r.Post("/new", s.UpsertNewQuery)
	return r
}

func (s *Controller) GetQueryHome(w http.ResponseWriter, r *http.Request) {
	res, err := s.conns.GetQueries(r.Context(), &connections.GetQueriesRequest{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.view.Render(w, r, http.StatusOK, "pages/query/index", res)
}

type QueryData struct {
	Query      string
	Params     []QueryParam
	FieldTypes []FieldType
	Results    []map[string]interface{}
	Keys       []string
	Slots      string
	Bytes      string
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

func (s *Controller) GetQueryNew(w http.ResponseWriter, r *http.Request) {
	data := QueryData{
		Query: "SELECT * FROM `table` LIMIT 10",
		Params: []QueryParam{
			{Name: "id", Type: "integer", ExampleValue: "123", Description: "User ID"},
		},
		FieldTypes: []FieldType{
			{Value: "string", Name: "String"},
			{Value: "integer", Name: "Integer"},
			{Value: "float", Name: "Float"},
		},
		Results: nil,
		Keys:    nil,
		Slots:   "10",
		Bytes:   "2048",
	}
	s.view.Render(w, r, http.StatusOK, "pages/query/new", data)
}

func (s *Controller) UpsertNewQuery(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
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

	res, err := s.conns.NewKey(r.Context(), &connections.NewKeyRequest{
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
