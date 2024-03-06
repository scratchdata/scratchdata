package api

import (
	"crypto/sha256"
	"os"

	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/database"
	"github.com/scratchdata/scratchdata/pkg/transport"

	"github.com/bwmarrin/snowflake"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

type API struct {
	config        config.API
	db            database.Database
	dataTransport transport.DataTransport

	app *fiber.App

	snow *snowflake.Node
}

func NewAPIServer(config config.API, db database.Database, dataTransport transport.DataTransport) *API {
	rc := &API{
		config:        config,
		db:            db,
		dataTransport: dataTransport,
	}
	return rc
}

func (a *API) Start() error {
	log.Info().Msg("Starting API")

	// Get the current hostname
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	// Hash the hostname using SHA-256
	hash := sha256.Sum256([]byte(hostname))

	// Convert the last byte of the hash to uint32, but we only need the lower 10 bits
	// Note: The hash is a byte array, and we are only working with the last byte for simplicity
	lastByte := hash[len(hash)-1]          // Get the last byte of the hash
	lower10Bits := int64(lastByte) & 0x3FF // Mask to get lower 10 bits

	node, err := snowflake.NewNode(lower10Bits)
	if err != nil {
		return err
	}
	a.snow = node

	err = os.MkdirAll(a.config.DataDir, os.ModePerm)
	if err != nil {
		log.Fatal().Err(err).Str("path", a.config.DataDir).Msg("Unable to create data ingest directory")
	}

	err = a.dataTransport.StartProducer()
	if err != nil {
		return err
	}

	err = a.InitializeAPIServer()
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
