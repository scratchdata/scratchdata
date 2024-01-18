package api

import (
	"errors"
	"io"
	"scratchdata/models"
	"scratchdata/pkg/destinations"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
)

type WriteResult struct {
	Headers map[string]string
	Error   error
}

func (i *API) queryAsync(query string, connection destinations.DatabaseServer, writer io.WriteCloser, c chan WriteResult) {
	headers, err := connection.QueryJSON(query, writer)
	writer.Close()

	result := WriteResult{Headers: headers, Error: err}
	c <- result
	close(c)
}

func (i *API) Query(c *fiber.Ctx) error {
	query := utils.CopyString(c.Query("q"))

	if c.Method() == "POST" {
		payload := struct {
			Query string `json:"query"`
		}{}

		if err := c.BodyParser(&payload); err != nil {
			return err
		}

		query = payload.Query
	}

	apiKey := c.Locals("apiKey").(models.APIKey)

	// TODO: read-only vs read-write connections
	connectionSetting := i.db.GetDatabaseConnection(apiKey.DestinationID)

	if connectionSetting.ID == "" {
		return errors.New("No DB Connections set up")
	}

	// TODO: need some sort of local connection pool or storage
	connection, err := destinations.GetDestination(connectionSetting)
	if err != nil {
		return errors.New("Destination " + connectionSetting.Type + " does not exist")
	}

	// TODO: use a buffered pipe of some sort to stream results
	// https://github.com/gofiber/fiber/issues/1034
	// https://stackoverflow.com/questions/68778961/how-to-configure-the-buffer-size-for-http-responsewriter

	ch := make(chan WriteResult)
	pipeReader, pipeWriter := io.Pipe()

	go i.queryAsync(query, connection, pipeWriter, ch)

	_, err = io.Copy(c.Context().Response.BodyWriter(), pipeReader)
	if err != nil {
		return err
	}

	res := <-ch

	c.Set("Content-type", "application/json")
	for k, v := range res.Headers {
		c.Set(k, v)
	}

	return res.Error
}

func (i *API) Tables(c *fiber.Ctx) error {
	type t struct {
		Name string `json:"name"`
	}
	return c.JSON([]t{t{Name: "log"}, t{Name: "test_table"}})
}
