package view

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"

	"github.com/foolin/goview"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/csrf"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/destinations"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/view/templates"
)

type Connections struct {
	Destinations []config.Destination
}

type UpsertConnection struct {
	Destination config.Destination
}

type Connect struct {
	InsertExample string
}

type Model struct {
	CSRFToken        template.HTML
	Email            string
	Connect          Connect
	Connections      Connections
	UpsertConnection UpsertConnection
}

func embeddedFH(config goview.Config, tmpl string) (string, error) {
	bytes, err := templates.Templates.ReadFile(tmpl + config.Extension)
	return string(bytes), err
}

func New(
	storageServices *storage.Services,
	c config.DashboardConfig,
	destManager *destinations.DestinationManager,
	auth func(h http.Handler) http.Handler,
) (*chi.Mux, error) {
	r := chi.NewRouter()

	csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))

	// TODO: Want to be able to disable this for quick local dev
	r.Use(auth)
	r.Use(csrfMiddleware)

	gv := goview.New(goview.Config{
		Root:         "pkg/view/templates",
		Extension:    ".html",
		Master:       "layout/base",
		DisableCache: true,
		Funcs: map[string]any{
			"prettyPrint": func(data any) string {
				bytes, err := json.MarshalIndent(data, "", "    ")
				if err != nil {
					return err.Error()
				}
				return string(bytes)
			},
			"cond": func(a bool, b, c any) any {
				if a {
					return b
				}
				return c
			},
		},
	})
	if !c.LiveReload {
		gv.SetFileHandler(embeddedFH)
	}

	getUser := func(r *http.Request) (*models.User, bool) {
		userAny := r.Context().Value("user")
		user, ok := userAny.(*models.User)
		return user, ok
	}

	loadModel := func(r *http.Request) Model {
		user, ok := getUser(r)
		m := Model{
			CSRFToken: csrf.TemplateField(r),
		}
		if !ok {
			return m
		}
		m.Email = user.Email
		return m
	}

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/index", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections", func(w http.ResponseWriter, r *http.Request) {
		user, ok := getUser(r)
		if !ok {
			http.Error(w, "User not found", http.StatusInternalServerError)
			return
		}
		destinations, err := storageServices.Database.GetDestinations(r.Context(), user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		m := loadModel(r)
		m.Connections = Connections{
			Destinations: destinations,
		}
		err = gv.Render(w, http.StatusOK, "pages/connections/index", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections/new", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/connections/upsert", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Post("/connections/upsert", func(w http.ResponseWriter, r *http.Request) {
		user, ok := getUser(r)
		if !ok {
			http.Error(w, "User not found", http.StatusInternalServerError)
			return
		}

		err := r.ParseForm()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		id := r.Form.Get("id")
		if id != "" {
			http.Error(w, "Editing connections not yet supported", http.StatusBadRequest)
			return
		}

		settings := map[string]any{}
		name := r.Form.Get("name")

		t := r.Form.Get("type")
		switch t {
		case "duckdb":
			// XXX breadchris support file destination?

			tok := r.Form.Get("token")
			if tok == "" {
				http.Error(w, "Must specify a token", http.StatusBadRequest)
				return
			}
			db := r.Form.Get("database")
			if db == "" {
				http.Error(w, "Must specify a token", http.StatusBadRequest)
				return
			}

			// XXX breadchris what validation should be done here?

			settings["token"] = tok
			settings["database"] = db
		default:
			http.Error(w, "Unknown connection type", http.StatusBadRequest)
			return
		}

		cd := config.Destination{
			Type:     t,
			Settings: settings,
		}

		err = destManager.TestCredentials(cd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		teamId, err := storageServices.Database.GetTeamId(user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		dest, err := storageServices.Database.CreateDestination(r.Context(), teamId, name, t, settings)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		key := uuid.New().String()
		hashedKey := storageServices.Database.Hash(key)
		err = storageServices.Database.AddAPIKey(r.Context(), dest.ID, hashedKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		m := loadModel(r)
		m.Connect.InsertExample = fmt.Sprintf(
			"curl -X POST '%s/api/data/insert/%s?api_key=%s' --json '{\"user\": \"bob\", \"event\": \"click\"}'",
			c.ExternalURL,
			dest.Name,
			key,
		)

		err = gv.Render(w, http.StatusOK, "pages/connections/api", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections/new/{type}", func(w http.ResponseWriter, r *http.Request) {
		m := loadModel(r)
		m.UpsertConnection = UpsertConnection{
			Destination: config.Destination{
				ID:   -1,
				Type: chi.URLParam(r, "type"),
			},
		}
		err := gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Get("/connections/edit/{id}", func(w http.ResponseWriter, r *http.Request) {
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

		destinations, err := storageServices.Database.GetDestinations(r.Context(), user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var dest config.Destination
		for _, d := range destinations {
			if d.ID == id {
				dest = d
				break
			}
		}

		if dest.ID == 0 {
			http.Error(w, "Destination not found", http.StatusNotFound)
			return
		}

		m := loadModel(r)
		m.UpsertConnection = UpsertConnection{
			Destination: dest,
		}
		err = gv.Render(w, http.StatusOK, "pages/connections/upsert", m)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	r.Post("/connections/delete", func(w http.ResponseWriter, r *http.Request) {
		idStr := r.Form.Get("id")
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

		teamId, err := storageServices.Database.GetTeamId(user.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = storageServices.Database.GetDestination(r.Context(), teamId, id)
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

	r.Get("/keys", func(w http.ResponseWriter, r *http.Request) {
		err := gv.Render(w, http.StatusOK, "pages/keys/index", loadModel(r))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	return r, nil
}
