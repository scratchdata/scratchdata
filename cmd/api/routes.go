package api

import (
	"errors"
	"fmt"
	"scratchdata/pkg/destinations"

	"github.com/gofiber/contrib/fiberzerolog"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func (i *API) Query(c *fiber.Ctx) error {
	query := utils.CopyString(c.Query("q"))
	connections := i.db.GetDatabaseConnections("x")
	connectionSetting := connections[0]

	// TODO: need some sort of local connection pool or storage
	connection := destinations.GetDestination(connectionSetting)
	if connection == nil {
		return errors.New("Destination " + connectionSetting.Type + " does not exist")
	}

	c.Set("Content-type", "application/json")

	// TODO: use a buffered pipe of some sort to stream results
	// https://github.com/gofiber/fiber/issues/1034
	// https://stackoverflow.com/questions/68778961/how-to-configure-the-buffer-size-for-http-responsewriter
	err := connection.QueryJSON(query, c.Context().Response.BodyWriter())

	return err

	// if c.Method() == "POST" {
	// 	payload := struct {
	// 		Query string `json:"query"`
	// 	}{}

	// 	if err := c.BodyParser(&payload); err != nil {
	// 		return err
	// 	}

	// 	query = payload.Query
	// }

	// format := utils.CopyString(c.Query("format", "json"))
	// api_key, _ := i.getField("X-API-KEY", "api_key", "", c)
	// keyDetails, ok := i.apiKeys.GetDetailsByKey(api_key)
	// if !ok {
	// 	return fiber.NewError(fiber.StatusUnauthorized)
	// }

	// chosenServer, err := i.chooser.ChooseServerForReading(i.serverManager, keyDetails)
	// if err != nil {
	// 	return err
	// }

	// resp, err := i.query(keyDetails, chosenServer, query, format)
	// if err != nil {
	// 	return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// }

	// defer resp.Body.Close()

	// if resp.StatusCode == 403 {
	// 	return fiber.NewError(fiber.StatusUnauthorized)
	// } else if resp.StatusCode != 200 {
	// 	msg, _ := io.ReadAll(resp.Body)
	// 	return fiber.NewError(fiber.StatusBadRequest, string(msg))
	// }

	// switch format {
	// case "html":
	// 	md, _ := io.ReadAll(resp.Body)
	// 	// create markdown parser with extensions
	// 	extensions := parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock
	// 	p := parser.NewWithExtensions(extensions)
	// 	doc := p.Parse(md)

	// 	// create HTML renderer with extensions
	// 	htmlFlags := html.CommonFlags | html.HrefTargetBlank
	// 	opts := html.RendererOptions{Flags: htmlFlags}
	// 	renderer := html.NewRenderer(opts)

	// 	html := markdown.Render(doc, renderer)
	// 	c.Set(fiber.HeaderContentType, fiber.MIMETextHTML)
	// 	c.WriteString(`
	// 	<style>
	// 	table, tr, td, th {border: 1px solid; border-collapse:collapse}
	// 	td,th{padding:3px;}
	// 	</style>
	// 	`)
	// 	c.Write(html)
	// 	return nil
	// default:
	// 	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

	// 	c.WriteString("[")

	// 	// Treat the output as a linked list of text fragments.
	// 	// Each fragment could be a partial JSON line
	// 	var nextIsPrefix = true
	// 	var nextErr error = nil
	// 	var nextLine []byte
	// 	reader := bufio.NewReader(resp.Body)
	// 	line, isPrefix, err := reader.ReadLine()

	// 	for {
	// 		// If we're at the end of our input, break
	// 		if err == io.EOF {
	// 			break
	// 		} else if err != nil {
	// 			return err
	// 		}

	// 		// Output the data
	// 		c.Write(line)

	// 		// Check to see whether we are at the last row by looking for EOF
	// 		nextLine, nextIsPrefix, nextErr = reader.ReadLine()

	// 		// If the next row is not an EOF, then output a comma. This is to avoid a
	// 		// trailing comma in our JSON
	// 		if !isPrefix && nextErr != io.EOF {
	// 			c.WriteString(",")
	// 		}

	// 		// Equivalent of "currentPointer = currentPointer.next"
	// 		line, isPrefix, err = nextLine, nextIsPrefix, nextErr
	// 	}
	// 	c.WriteString("]")
	// }
}

func (a *API) InitializeAPIServer() error {
	app := fiber.New()
	a.app = app

	app.Use(fiberzerolog.New(fiberzerolog.Config{
		Logger: &log.Logger,
		Levels: []zerolog.Level{zerolog.ErrorLevel, zerolog.WarnLevel, zerolog.TraceLevel},
	}))

	a.app.Get("/query", a.Query)
	a.app.Post("/query", a.Query)

	err := app.Listen(fmt.Sprintf(":%d", +a.config.Port))
	if err != nil {
		return err
	}

	return nil
}
