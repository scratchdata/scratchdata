package api

import (
	"errors"
	"scratchdata/models"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
)

const TABLE_NAME_HEADER = "X-SCRATCHDB-TABLE"
const TABLE_NAME_QUERY = "table"
const TABLE_NAME_JSON = "table"

const FLATTEN_HEADER = "X-SCRATCHDB-FLATTEN"
const FLATTEN_QUERY = "flatten"
const FLATTEN_JSON = "flatten"

func (a *API) getTableName(c *fiber.Ctx) (string, string) {
	if c.Get(TABLE_NAME_HEADER) != "" {
		return utils.CopyString(c.Get(TABLE_NAME_HEADER)), "header"
	}

	if c.Query(TABLE_NAME_QUERY) != "" {
		return utils.CopyString(c.Query(TABLE_NAME_QUERY)), "query"
	}

	return gjson.GetBytes(c.Body(), TABLE_NAME_JSON).String(), "body"
}

func (a *API) getFlattenType(c *fiber.Ctx) (string, string) {
	if c.Get(FLATTEN_HEADER) != "" {
		return utils.CopyString(c.Get(FLATTEN_HEADER)), "header"
	}

	if c.Query(FLATTEN_QUERY) != "" {
		return utils.CopyString(c.Query(FLATTEN_QUERY)), "query"
	}

	return gjson.GetBytes(c.Body(), FLATTEN_JSON).String(), "body"
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

	// TODO: this block can be abstracted as we also use it for query
	/////
	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := a.db.GetDatabaseConnection(apiKey.DestinationID)

	if connectionSetting.ID == "" {
		return errors.New("No DB Connections set up")
	}
	/////

	input := c.Body()
	if !gjson.ValidBytes(input) {
		return errors.New("Invalid JSON")
	}

	// TODO: Use actual table name from request
	tableName := "t"

	var FlattenFunc = Flatten

	parsed := gjson.ParseBytes(input)
	if parsed.IsArray() {
		for _, item := range parsed.Array() {
			flat, err := FlattenFunc(item.Get(`@ugly`).Raw)
			if err != nil {
				log.Error().Err(err).Str("json", item.Str).Msg("Unable to flatten json")
			}
			a.dataTransport.Write(connectionSetting.ID, tableName, []byte(flat))
		}
	} else {
		flat, err := FlattenFunc(gjson.GetBytes(input, `@ugly`).Raw)
		if err != nil {
			return err
		}
		a.dataTransport.Write(connectionSetting.ID, tableName, []byte(flat))
	}

	return c.SendString("ok")
}
