package api

import (
	"errors"
	"net/http"

	"github.com/jeremywohl/flatten"
	"scratchdata/models"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

const TableNameHeader = "X-SCRATCHDB-TABLE"
const TableNameQuery = "table"
const TableNameJson = "table"

const FlattenHeader = "X-SCRATCHDB-FLATTEN"
const FlattenQuery = "flatten"
const FlattenJson = "flatten"

func (a *API) getFlattenType(c *fiber.Ctx) (string, string) {
	if c.Get(FlattenHeader) != "" {
		return utils.CopyString(c.Get(FlattenHeader)), "header"
	}

	if c.Query(FlattenQuery) != "" {
		return utils.CopyString(c.Query(FlattenQuery)), "query"
	}

	return gjson.GetBytes(c.Body(), FlattenJson).String(), "body"
}

func (a *API) Insert(c *fiber.Ctx) error {
	if c.QueryBool("debug", false) {
		rid := ulid.Make().String()
		log.Debug().
			Str("request_id", rid).
			Interface("headers", c.GetReqHeaders()).
			Str("body", string(c.Body())).
			Interface("queryParams", c.Queries()).
			Msg("Incoming request")
	}

	body := c.Body()
	if !gjson.ValidBytes(body) {
		return fiber.NewError(http.StatusBadRequest, "invalid JSON")
	}

	// TODO: this block can be abstracted as we also use it for query
	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := a.db.GetDatabaseConnection(apiKey.DestinationID)
	if connectionSetting.ID == "" {
		return fiber.NewError(http.StatusUnauthorized, "no connection is set up")
	}

	var (
		tableName, flatAlgo string
		fromBody            bool
	)
	flatAlgo = c.Get(FlattenHeader, c.Query(FlattenQuery))
	tableName = c.Get(TableNameHeader, c.Query(TableNameQuery))
	if tableName == "" {
		if tableName = gjson.GetBytes(body, TableNameJson).String(); tableName == "" {
			return fiber.NewError(http.StatusBadRequest, "missing required table field")
		}
		fromBody = true
	}

	parsed := gjson.ParseBytes(body)
	if fromBody {
		if parsed = parsed.Get("data"); !parsed.Exists() {
			return fiber.NewError(http.StatusBadRequest, "missing required data field")
		}
	}

	var (
		lines []string
		err   error
	)
	if flatAlgo == "explode" {
		explodeJSON, explodeErr := ExplodeJSON(parsed)
		if explodeErr != nil {
			log.Err(explodeErr).Str("parsed", parsed.Raw).Msg("error exploding JSON")
			err = errors.Join(err, explodeErr)
		}
		lines = append(lines, explodeJSON...)
	} else {
		flat, err := flatten.FlattenString(
			parsed.Raw,
			"",
			flatten.UnderscoreStyle,
		)
		if err != nil {
			return fiber.NewError(http.StatusBadRequest, err.Error())
		}
		lines = append(lines, flat)
	}

	for _, line := range lines {
		writeErr := a.dataTransport.Write(connectionSetting.ID, tableName, []byte(line))
		if writeErr != nil {
			err = errors.Join(err, writeErr)
		}
	}
	if err != nil {
		return fiber.NewError(http.StatusExpectationFailed, err.Error())
	}

	return c.SendString("ok")
}
