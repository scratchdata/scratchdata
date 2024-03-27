package api

import (
	"errors"
	"net/http"
	"os"

	"github.com/go-chi/render"
	"github.com/rs/zerolog/log"
)

func (a *ScratchDataAPIStruct) Healthcheck(w http.ResponseWriter, r *http.Request) {
	_, err := os.Stat(a.config.HealthCheckFailFile)
	if err == nil {
		http.Error(w, "Status set to unhealthy", http.StatusServiceUnavailable)
		return
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Error().Err(err).Msg("Unable to check for unhealthy file")
	}

	// TODO: make sure disk is not full when ingesting
	// a.dataSink.Healthcheck

	render.PlainText(w, r, "ok")
}
