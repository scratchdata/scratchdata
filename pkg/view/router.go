package view

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/csrf"
	"github.com/gorilla/schema"
	"github.com/gorilla/sessions"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gorm.io/datatypes"
)

const gorillaSessionName = "gorilla_session"

type Connections struct {
	Destinations []config.Destination
}

type UpsertConnection struct {
	RequestID   string
	Destination config.Destination
	TypeDisplay string
	FormFields  []util.Form
}

type Connect struct {
	APIKey string
	APIUrl string
}

type ShareQuery struct {
	Expires string
	Name    string
	ID      string
}

type FlashType string

const (
	FlashTypeSuccess FlashType = "success"
	FlashTypeWarning FlashType = "warning"
	FlashTypeError   FlashType = "error"
)

type Flash struct {
	Type    FlashType
	Title   string
	Message string
	Fatal   bool
}

type Request struct {
	URL string
}

type Model struct {
	CSRFToken        template.HTML
	Email            string
	HideSidebar      bool
	Flashes          []Flash
	Connect          Connect
	Connections      Connections
	UpsertConnection UpsertConnection
	Data             map[string]any
	Request          Request
	ShareQuery       ShareQuery
}

func init() {
	gob.Register(Flash{})
}

func embeddedFH(config goview.Config, tmpl string) (string, error) {
	bytes, err := templates.Templates.ReadFile(tmpl + config.Extension)
	return string(bytes), err
}

func newViewEngine(liveReload bool) *goview.ViewEngine {
	gv := goview.New(goview.Config{
		Root:         "pkg/view/templates",
		Extension:    ".html",
		Master:       "layout/base",
		Partials:     []string{"partials/flash"},
		DisableCache: true,
		Funcs: map[string]any{
			"prettyPrint": func(data any) string {
				bytes, err := json.MarshalIndent(data, "", "    ")
				if err != nil {
					return err.Error()
				}
				return string(bytes)
			},
			"title": func(a string) string {
				return cases.Title(language.AmericanEnglish).String(a)
			},
		},
	})
	if !liveReload {
		gv.SetFileHandler(embeddedFH)
	}
	return gv
}

func New(
	storageServices *storage.Services,
	c config.DashboardConfig,
	destManager *destinations.DestinationManager,
	auth func(h http.Handler) http.Handler,
) (*chi.Mux, error) {
	connRouter := chi.NewRouter()
	reqRouter := chi.NewRouter()
	homeRouter := chi.NewRouter()

	csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))
	sessionStore := sessions.NewCookieStore([]byte(c.CSRFSecret))

	homeRouter.Use(auth)

	// TODO: Want to be able to disable this for quick local dev
	connRouter.Use(auth)
	connRouter.Use(csrfMiddleware)

	reqRouter.Use(csrfMiddleware)

	formDecoder := schema.NewDecoder()
	formDecoder.IgnoreUnknownKeys(true)

	gv := newViewEngine(c.LiveReload)

	getUser := func(r *http.Request) (*models.User, bool) {
		userAny := r.Context().Value("user")
		user, ok := userAny.(*models.User)
		return user, ok
	}

	loadModel := func(r *http.Request, w http.ResponseWriter, data ...map[string]any) Model {
		// TODO breadchris how should these errors be handled?
		session, err := sessionStore.Get(r, gorillaSessionName)
		if err != nil {
			log.Err(err).Msg("failed to get session")
		}
		flashes := session.Flashes()
		err = session.Save(r, w)
		if err != nil {
			log.Err(err).Msg("failed to save session")
		}

		var fls []Flash
		for _, flash := range flashes {
			f, ok := flash.(Flash)
			if !ok {
				continue
			}
			fls = append(fls, f)
		}

		m := Model{
			CSRFToken: csrf.TemplateField(r),
			Flashes:   fls,
		}

		user, ok := getUser(r)
		if !ok {
			return m
		}
		m.Email = user.Email

		if len(data) > 0 {
			m.Data = data[0]
		}

		return m
	}

	newFlash := func(w http.ResponseWriter, r *http.Request, f Flash) {
		// TODO breadchris how should these errors be handled?
		session, err := sessionStore.Get(r, gorillaSessionName)
		if err != nil {
			log.Err(err).Msg("failed to get session")
			return
		}
		session.AddFlash(f)
		err = session.Save(r, w)
		if err != nil {
			log.Err(err).Msg("failed to save session")
		}
	}

	type FormState struct {
		Name        string
		Type        string
		Settings    map[string]any
		RequestID   string
		HideSidebar bool
	}

	renderNewConnection := func(w http.ResponseWriter, r *http.Request, m Model) {
		err := gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	flashAndRender := func(w http.ResponseWriter, r *http.Request, f Flash, form FormState) {
		log.Info().Interface("flash", f).Msg("failed to create destination")
		newFlash(w, r, f)

		m := loadModel(r, w)
		if f.Fatal {
			err := gv.Render(w, http.StatusInternalServerError, "pages/connections/fatal", m)
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
		renderNewConnection(w, r, m)
	}

	getTeamId := func(r *http.Request) (uint, error) {
		user, ok := getUser(r)
		if !ok {
			return 0, errors.New("user not found")
		}

		teamId, err := storageServices.Database.GetTeamId(user.ID)
		if err != nil {
			return 0, err
		}

		return teamId, nil
	}

	validateRequestId := func(c context.Context, requestId string) (models.ConnectionRequest, error) {
		_, err := uuid.Parse(requestId)
		if err != nil {
			return models.ConnectionRequest{}, errors.New("invalid request id")
		}

		req, err := storageServices.Database.GetConnectionRequest(c, uuid.MustParse(requestId))
		if err != nil {
			return models.ConnectionRequest{}, errors.New("failed to lookup request")
		}

		if req.Expiration.Before(time.Now()) {
			// TODO breadchris if the request is expired, suggest creating a new one
			return models.ConnectionRequest{}, errors.New("request expired")
		}
		return req, nil
	}

	reqRouter.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
		requestId := chi.URLParam(r, "id")

		if requestId == "" {
			http.Error(w, "Request ID required", http.StatusBadRequest)
			return
		}
		connReq, err := validateRequestId(r.Context(), requestId)
		if err != nil {
			newFlash(w, r, Flash{
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

		m := loadModel(r, w)
		m.UpsertConnection = UpsertConnection{
			Destination: connReq.Destination.ToConfig(),
			RequestID:   requestId,
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		m.HideSidebar = true

		err = gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	homeRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/index", loadModel(r, w))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		destModels, err := storageServices.Database.GetDestinations(r.Context(), teamId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		dests := []config.Destination{}
		for _, d := range destModels {
			dests = append(dests, d.ToConfig())
		}

		m := loadModel(r, w)
		m.Connections = Connections{
			Destinations: dests,
		}
		err = gv.Render(w, http.StatusOK, "pages/connections/index", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Get("/new", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/connections/new", loadModel(r, w))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/request", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := getTeamId(r)
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

		dest, err := storageServices.Database.CreateDestination(
			r.Context(), teamId, name, t, map[string]any{},
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		req, err := storageServices.Database.CreateConnectionRequest(r.Context(), dest)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if req.ID == 0 {
			http.Error(w, "Failed to create request", http.StatusInternalServerError)
			return
		}

		m := loadModel(r, w)

		m.Request = Request{
			URL: fmt.Sprintf(
				"%s/dashboard/request/%s",
				c.ExternalURL,
				req.RequestID,
			),
		}

		err = gv.Render(w, http.StatusOK, "pages/request/link", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	upsertConn := func(w http.ResponseWriter, r *http.Request, requireRequestID bool) {
		session, err := sessionStore.Get(r, gorillaSessionName)
		if err != nil {
			log.Err(err).Msg("failed to get session")
		}
		// get all flashes and clear them
		_ = session.Flashes()

		form := FormState{
			Settings: map[string]any{},
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

		form.Name = r.Form.Get("name")
		form.Type = r.Form.Get("type")
		form.RequestID = r.Form.Get("request_id")

		var (
			connReq models.ConnectionRequest
		)
		if requireRequestID {
			form.HideSidebar = true

			connReq, err = validateRequestId(r.Context(), form.RequestID)
			if err != nil {
				flashAndRender(w, r, Flash{
					Type:  FlashTypeError,
					Title: err.Error(),
				}, form)
				return
			}

		}

		vc, ok := destinations.ViewConfig[form.Type]
		if !ok {
			flashAndRender(w, r, Flash{
				Type:  FlashTypeError,
				Title: "Unknown connection type",
			}, form)
			return
		}

		instance := reflect.New(reflect.TypeOf(vc.Type)).Interface()

		form.Settings = map[string]any{}
		for k, v := range r.PostForm {
			if len(v) == 1 {
				form.Settings[k] = v[0]
			}
		}

		err = formDecoder.Decode(instance, r.PostForm)
		if err != nil {
			flashAndRender(w, r, Flash{
				Type:    FlashTypeError,
				Title:   "Failed to decode form",
				Message: err.Error(),
			}, form)
			return
		}

		var settings map[string]any
		err = mapstructure.Decode(instance, &settings)
		if err != nil {
			flashAndRender(w, r, Flash{
				Type:    FlashTypeError,
				Title:   "Failed to decode form",
				Message: err.Error(),
			}, form)
			return
		}

		cd := config.Destination{
			Type:     form.Type,
			Name:     form.Name,
			Settings: settings,
		}

		err = destManager.TestCredentials(cd)
		if err != nil {
			log.Err(err).Msg("failed to connect to destination")
			flashAndRender(w, r, Flash{
				Type:    FlashTypeError,
				Title:   "Failed to connect to destination. Check the settings and try again.",
				Message: err.Error(),
			}, form)
			return
		}

		var teamId uint
		if connReq.ID == 0 {
			teamId, err = getTeamId(r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
		} else {
			teamId = connReq.Destination.TeamID
		}

		m := loadModel(r, w)

		if connReq.ID != 0 {
			connReq.Destination.Name = form.Name
			connReq.Destination.Settings = datatypes.NewJSONType(settings)

			err = storageServices.Database.UpdateDestination(r.Context(), connReq.Destination)
			if err != nil {
				flashAndRender(w, r, Flash{
					Type:    FlashTypeError,
					Title:   "Failed to update destination",
					Message: err.Error(),
				}, form)
				return
			}

			err = storageServices.Database.DeleteConnectionRequest(r.Context(), connReq.ID)
			if err != nil {
				log.Err(err).Msg("failed to delete connection request")
			}

			http.Redirect(w, r, "/dashboard/request/success", http.StatusFound)
			return
		}

		dest, err := storageServices.Database.CreateDestination(
			r.Context(), teamId, form.Name, form.Type, settings,
		)
		if err != nil {
			flashAndRender(w, r, Flash{
				Type:    FlashTypeError,
				Title:   "Failed to create destination",
				Message: err.Error(),
			}, form)
			return
		}

		key := uuid.New().String()
		hashedKey := storageServices.Database.Hash(key)
		err = storageServices.Database.AddAPIKey(r.Context(), int64(dest.ID), hashedKey)
		if err != nil {
			flashAndRender(w, r, Flash{
				Type:    FlashTypeError,
				Title:   "Failed to create destination",
				Message: err.Error(),
			}, form)
			return
		}

		m.Connect.APIKey = key
		m.Connect.APIUrl = c.ExternalURL

		err = gv.Render(w, http.StatusOK, "pages/connections/api", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}

	reqRouter.Post("/upsert", func(w http.ResponseWriter, r *http.Request) {
		upsertConn(w, r, true)
	})

	reqRouter.Get("/success", func(w http.ResponseWriter, r *http.Request) {
		m := loadModel(r, w)
		m.HideSidebar = true
		err := gv.Render(w, http.StatusOK, "pages/request/success", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	connRouter.Post("/upsert", func(w http.ResponseWriter, r *http.Request) {
		upsertConn(w, r, false)
	})

	connRouter.Post("/keys", func(w http.ResponseWriter, r *http.Request) {
		teamId, err := getTeamId(r)
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

		dest, err := storageServices.Database.GetDestination(r.Context(), teamId, uint(destID))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		key := uuid.New().String()
		hashedKey := storageServices.Database.Hash(key)
		err = storageServices.Database.AddAPIKey(r.Context(), int64(dest.ID), hashedKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := loadModel(r, w)
		m.Connect.APIKey = key
		m.Connect.APIUrl = c.ExternalURL
		err = gv.Render(w, http.StatusOK, "pages/connections/api", m)
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

		m := loadModel(r, w)
		m.UpsertConnection = UpsertConnection{
			Destination: config.Destination{
				ID:   0,
				Type: t,
			},
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		renderNewConnection(w, r, m)
	})

	connRouter.Get("/edit/{id}", func(w http.ResponseWriter, r *http.Request) {
		idStr := chi.URLParam(r, "id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		user, ok := getUser(r)
		if !ok {
			http.Error(w, "User not found", http.StatusInternalServerError)
			return
		}

		dests, err := storageServices.Database.GetDestinations(r.Context(), user.ID)
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

		m := loadModel(r, w)
		m.UpsertConnection = UpsertConnection{
			Destination: dest.ToConfig(),
			TypeDisplay: vc.Display,
			FormFields:  util.ConvertToForms(vc.Type),
		}
		err = gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
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

		teamId, err := getTeamId(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = storageServices.Database.GetDestination(r.Context(), teamId, uint(id))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = storageServices.Database.DeleteDestination(r.Context(), teamId, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/dashboard/connections", http.StatusFound)
	})

	r := chi.NewRouter()
	r.Mount("/", homeRouter)
	r.Mount("/request", reqRouter)
	r.Mount("/connections", connRouter)

	return r, nil
}
