package api

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/tidwall/gjson"
)

const API_KEY_HEADER = "X-API-KEY"
const API_KEY_QUERY = "api_key"
const API_KEY_JSON = "api_key"

func (a *API) getAPIKey(c *fiber.Ctx) string {
	if c.Get(API_KEY_HEADER) != "" {
		return utils.CopyString(c.Get(API_KEY_HEADER))
	}

	if c.Query(API_KEY_QUERY) != "" {
		return utils.CopyString(c.Query(API_KEY_QUERY))
	}

	return gjson.GetBytes(c.Body(), API_KEY_JSON).String()
}

func (a *API) AuthMiddleware(c *fiber.Ctx) error {
	// Get API key from request
	apiKey := a.getAPIKey(c)

	keyDetails := a.db.GetAPIKeyDetails(a.db.Hash(apiKey))
	if keyDetails.ID == "" {
		return c.SendStatus(fiber.StatusUnauthorized)
	}

	// Set request
	c.Locals("apiKey", keyDetails)

	return c.Next()
}
