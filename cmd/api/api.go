package api

import (
	"fmt"
	"os"

	"github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/rs/zerolog"
	"scratchdata/config"
	"scratchdata/pkg/database"
	"scratchdata/pkg/transport"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

type API struct {
	config        config.API
	db            database.Database
	dataTransport transport.DataTransport

	app *fiber.App
}

func NewAPIServer(config config.API, db database.Database, dataTransport transport.DataTransport) *API {
	a := &API{
		config:        config,
		db:            db,
		dataTransport: dataTransport,
	}
	app := fiber.New()
	a.app = app

	app.Use(fiberzerolog.New(fiberzerolog.Config{
		Logger: &log.Logger,
		Levels: []zerolog.Level{zerolog.ErrorLevel, zerolog.WarnLevel, zerolog.TraceLevel},
	}))

	// Initialize default config
	app.Use(cors.New())

	a.app.Get("/healthcheck", a.AuthMiddleware, a.HealthCheck)
	a.app.Get("/query", a.AuthMiddleware, a.Query)
	a.app.Post("/query", a.AuthMiddleware, a.Query)
	a.app.Get("/tables", a.AuthMiddleware, a.Tables)
	a.app.Post("/data", a.AuthMiddleware, a.Insert)
	return a
}

func (a *API) Start() error {
	log.Info().Msg("Starting API")

	err := os.MkdirAll(a.config.DataDir, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Str("path", a.config.DataDir).Msg("Unable to create data ingest directory")
	}

	err = a.dataTransport.StartProducer()
	if err != nil {
		return err
	}

	err = a.app.Listen(fmt.Sprintf(":%d", a.config.Port))
	if err != nil {
		return err
	}

	return nil
}

func (a *API) Stop() error {
	log.Info().Msg("Stopping API")
	err := a.app.Shutdown()
	if err != nil {
		log.Error().Err(err).Msg("failed to stop server")
	}
	err = a.dataTransport.StopProducer()
	if err != nil {
		log.Error().Err(err).Msg("failed to stop server")
	}
	return nil
}
