package api

import (
	"os"

	"github.com/scratchdata/scratchdata/util"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

func (a *API) HealthCheck(c *fiber.Ctx) error {
	// Check if server has been manually marked as unhealthy
	_, err := os.Stat(a.config.HealthCheckPath)
	if !os.IsNotExist(err) {
		log.Error().Msg("Server marked as unhealthy")
		return fiber.ErrBadGateway
	}

	// Ensure we haven't filled up disk
	currentFreeSpace := util.FreeDiskSpace(a.config.DataDir)
	if currentFreeSpace <= uint64(a.config.FreeSpaceRequiredBytes) {
		log.Error().Msg("Out of disk, failing health check")
		return fiber.ErrBadGateway
	}

	if err := a.db.HealthCheck(); err != nil {
		log.Error().Err(err).Msg("unhealthy DB")
		return fiber.ErrBadGateway
	}

	return c.SendString("ok")
}
