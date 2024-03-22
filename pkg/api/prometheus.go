package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
)

func PrometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)

		duration := time.Since(start)
		routeName := chi.RouteContext(r.Context()).RoutePattern()

		latency.WithLabelValues(routeName, strconv.Itoa(ww.Status())).Observe(duration.Seconds())
		responseSize.WithLabelValues(routeName).Observe(float64(ww.BytesWritten()))
	})
}
