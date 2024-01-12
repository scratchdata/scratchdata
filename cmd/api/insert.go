package api

import (
	"net/http"

	"scratchdata/models"

	"github.com/gofiber/fiber/v2"
	"github.com/oklog/ulid/v2"
	"github.com/rs/zerolog/log"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	TableNameHeader = "X-SCRATCHDB-TABLE"
	TableNameQuery  = "table"
	TableNameJson   = "table"

	FlattenHeader = "X-SCRATCHDB-FLATTEN"
	FlattenQuery  = "flatten"
	FlattenJson   = "flatten"
)

type DataSource int

const (
	Undefined DataSource = iota
	InHeader
	InQuery
	InBody
)

type DataTarget struct {
	Source DataSource
	Name   string
}

type Data struct {
	Source DataSource
	Value  string
}

func lookUp(c *fiber.Ctx, targets ...DataTarget) Data {
	for _, target := range targets {
		var value string
		switch target.Source {
		case Undefined:
			continue
		case InHeader:
			value = c.Get(target.Name)
		case InQuery:
			value = c.Query(target.Name)
		case InBody:
			value = gjson.GetBytes(c.Body(), target.Name).String()
		}
		if value != "" {
			return Data{
				Value:  value,
				Source: target.Source,
			}
		}
	}
	return Data{}
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

	flatAlgoData := lookUp(c,
		DataTarget{InHeader, FlattenHeader},
		DataTarget{InQuery, FlattenQuery},
		DataTarget{InBody, FlattenJson},
	)

	tableNameData := lookUp(c,
		DataTarget{InHeader, TableNameHeader},
		DataTarget{InQuery, TableNameQuery},
		DataTarget{InBody, TableNameJson},
	)
	if tableNameData.Source == Undefined || tableNameData.Value == "" {
		return fiber.NewError(http.StatusBadRequest, "missing required table field")
	}

	parsed := gjson.ParseBytes(body)
	if tableNameData.Source == InBody {
		if parsed = parsed.Get("data"); !parsed.Exists() {
			return fiber.NewError(http.StatusBadRequest, "missing required data field")
		}
	}

	var flattener Flattener
	if flatAlgoData.Value == "explode" {
		flattener = ExplodeFlattener{}
	} else {
		flattener = HorizontalFlattener{}
	}

	lines := parsed.Array()
	errorItems := map[int]bool{}
	for i, line := range lines {
		flatItems, err := flattener.Flatten(tableNameData.Value, line.Raw)
		if err != nil {
			errorItems[i] = true
			log.Trace().Err(err).Str("json", line.Raw).Msg("Unable to flatten JSON")
			continue
		}

		for _, flatItem := range flatItems {
			var writeErr error
			var toWrite string

			toWrite = flatItem.JSON

			if !gjson.Get(flatItem.JSON, "__row_id").Exists() {
				rowID := ulid.Make().String()
				if toWrite, err = sjson.Set(flatItem.JSON, "__row_id", rowID); err != nil {
					log.Trace().Err(err).Str("json", flatItem.JSON).Msg("Unable to add __row_id")
				}
			}

			writeErr = a.dataTransport.Write(connectionSetting.ID, flatItem.Table, []byte(toWrite))

			if writeErr != nil {
				errorItems[i] = true
				log.Trace().Err(err).Str("json", flatItem.JSON).Msg("Unable to write JSON")
			}
		}
	}

	if len(errorItems) > 0 {
		if len(errorItems) == len(lines) {
			return fiber.NewError(fiber.StatusBadRequest, "Unable to insert data")
		} else {
			return fiber.NewError(fiber.StatusBadRequest, "Partially inserted data")
		}
	}

	return c.SendString("ok")
}
