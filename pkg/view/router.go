package view

// import (
// 	"embed"
// 	"fmt"
// 	"io/fs"
// 	"net/http"
// 	"path"
// 	"strings"

// 	"github.com/rs/zerolog/log"

// 	"github.com/go-chi/chi/v5"
// 	"github.com/scratchdata/scratchdata/pkg/config"
// 	"github.com/scratchdata/scratchdata/pkg/destinations"
// 	"github.com/scratchdata/scratchdata/pkg/storage"
// 	"github.com/scratchdata/scratchdata/pkg/view/static"
// )

// // go:embed dist/*
// var spaFiles embed.FS

// func MyMiddleware(next http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		// create new context from `r` request context, and assign key `"user"`
// 		// to value of `"123"`

// 		log.Print("THIS IS MY MIDDLEWARE")
// 		// http.Error(w, "nope", 400)
// 		// return

// 		// call the next handler in the chain, passing the response writer and
// 		// the updated request object with the new context value.
// 		//
// 		// note: context.Context values are nested, so any previously set
// 		// values will be accessible as well, and the new `"user"` key
// 		// will be accessible from this point forward.
// 		next.ServeHTTP(w, r)
// 	})
// }

// func MountRoutes(
// 	r chi.Router,
// 	storageServices *storage.Services,
// 	c config.DashboardConfig,
// 	destManager *destinations.DestinationManager,
// 	auth func(h http.Handler) http.Handler,
// ) error {
// 	// csrfMiddleware := csrf.Protect([]byte(c.CSRFSecret))
// 	// sessionStore := sessions.NewCookieStore([]byte(c.CSRFSecret))

// 	// sessionService := session.NewSession(sessionStore)
// 	// view := NewView(sessionService, c.LiveReload)

// 	// connService := connections.NewService(
// 	// 	c,
// 	// 	storageServices,
// 	// 	destManager,
// 	// )
// 	// controller := NewController(
// 	// 	sessionService,
// 	// 	connService,
// 	// 	view,
// 	// )

// 	// r.Get("/share/{uuid}", func(w http.ResponseWriter, r *http.Request) {
// 	// 	queryUUID := chi.URLParam(r, "uuid")

// 	// 	id, err := uuid.Parse(queryUUID)
// 	// 	if err != nil {
// 	// 		http.Error(w, err.Error(), http.StatusBadRequest)
// 	// 		return
// 	// 	}

// 	// 	cachedQuery, found := storageServices.Database.GetShareQuery(r.Context(), id)
// 	// 	if !found {
// 	// 		http.Error(w, "Query not found", http.StatusNotFound)
// 	// 		return
// 	// 	}

// 	// 	year, month, day := cachedQuery.ExpiresAt.Date()

// 	// 	res := ShareQuery{
// 	// 		Expires: fmt.Sprintf("%s %d, %d", month.String(), day, year),
// 	// 		ID:      id.String(),
// 	// 		Name:    cachedQuery.Name,
// 	// 	}
// 	// 	view.RenderExternal(w, r, http.StatusOK, "pages/share", res)
// 	// })

// 	if c.Enabled {
// 		fileServer := http.FileServer(http.FS(static.Static))
// 		r.Handle("/static/*", http.StripPrefix("/static", fileServer))

// 		r.Handle("/", SPAHandler("/"))
// 		r.Handle("/*", SPAHandler("/"))

// 		r.Handle("/dashboard/*", auth(SPAHandler("/dashboard")))
// 		r.Handle("/dashboard", auth(SPAHandler("/dashboard")))

// 		// r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
// 		// 	http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
// 		// })

// 		// r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
// 		// 	http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
// 		// })
// 		// r.Mount("/request", controller.RequestRoutes(csrfMiddleware))
// 		// r.Route("/dashboard", func(r chi.Router) {
// 		// 	r.Mount("/", controller.HomeRoute(auth))
// 		// 	r.Mount("/connections", controller.ConnRoutes(auth, csrfMiddleware))
// 		// })
// 	}
// 	return nil
// }
