package api

import (
	"context"
	"scratchdata/pkg/accounts"
	"scratchdata/pkg/transport"

	"github.com/rs/zerolog/log"
)

type API struct {
	ctx            context.Context
	accountManager accounts.AccountManagement
	dataTransport  transport.DataTransport
}

func NewAPIServer(accountManager accounts.AccountManagement, dataTransport transport.DataTransport) *API {
	rc := &API{
		accountManager: accountManager,
		dataTransport:  dataTransport,
	}
	return rc
}

func (a *API) Start() error {
	log.Info().Msg("Starting API")
	a.dataTransport.StartProducer()
	return nil
}

func (a *API) Stop() error {
	log.Info().Msg("Stopping API")
	a.dataTransport.StopProducer()
	return nil
}
