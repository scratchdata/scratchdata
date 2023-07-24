package main

import (
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/jeremywohl/flatten"
	"github.com/spyzhov/ajson"
	"golang.org/x/crypto/acme/autocert"
)

func toJSON(rows *sql.Rows, c *fiber.Ctx) error {
	columnTypes, err := rows.ColumnTypes()

	if err != nil {
		return err
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {

		scanArgs := make([]interface{}, count)

		for i, v := range columnTypes {
			// log.Println(v.Name())
			// log.Println(v.DatabaseTypeName())

			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
				break
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
				break
			case "BIGINT":
				scanArgs[i] = new(sql.NullInt64)
				break
			case "UBIGINT":
				scanArgs[i] = new(sql.NullInt64)
				break
			case "DOUBLE":
				scanArgs[i] = new(sql.NullFloat64)
				break
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}

		err := rows.Scan(scanArgs...)

		if err != nil {
			return err
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {

			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[v.Name()] = z.Bool
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[v.Name()] = z.Int32
				continue
			}

			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	z, err := json.Marshal(finalRows)
	if err != nil {
		return err
	}

	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	_, err = c.Write(z)

	return err

}

func toHTML(rows *sql.Rows, c *fiber.Ctx) error {
	// Get column info from result
	columns, err := rows.Columns()
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	// Output table of data
	c.Set(fiber.HeaderContentType, fiber.MIMETextHTML)

	c.WriteString("<table border=1 cellpadding=3 cellspacing=0>")
	c.WriteString("<thead>")
	c.WriteString("<tr>")
	for _, col := range columns {
		c.WriteString("<th>" + col + "</th>")
	}
	c.WriteString("</tr>")
	c.WriteString("</thead>")
	values := make([]interface{}, len(columns))

	c.WriteString("<tbody>")

	for rows.Next() {
		c.WriteString("<tr>")
		for i := range values {
			values[i] = new(sql.NullString)
		}
		err := rows.Scan(values...)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		for i := range values {
			s := values[i].(*sql.NullString)
			c.WriteString("<td>" + s.String + "</td>")
		}
		c.WriteString("</tr>")
	}
	c.WriteString("</tbody>")
	c.WriteString("</table>")
	return nil
}

func runSSL(app *fiber.App) {
	host := os.Getenv("SCRATCHDB_HOST")
	if host == "" {
		panic("Must specify host for SSL")
	}
	// Certificate manager
	m := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		// Replace with your domain
		HostPolicy: autocert.HostWhitelist(host),
		// Folder to store the certificates
		Cache: autocert.DirCache("./certs"),
	}

	// TLS Config
	cfg := &tls.Config{
		// Get Certificate from Let's Encrypt
		GetCertificate: m.GetCertificate,
		// By default NextProtos contains the "h2"
		// This has to be removed since Fasthttp does not support HTTP/2
		// Or it will cause a flood of PRI method logs
		// http://webconcepts.info/concepts/http-method/PRI
		NextProtos: []string{
			"http/1.1", "acme-tls/1",
		},
	}
	ln, err := tls.Listen("tcp", ":443", cfg)
	if err != nil {
		panic(err)
	}

	// Start server
	log.Fatal(app.Listener(ln))

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) < 3 {
		log.Println("Must specify database file name and log file name")
		os.Exit(1)
	}

	logfile, err := os.OpenFile(os.Args[2], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	api_key := os.Getenv("API_KEY")

	// Create DB connection
	filename := os.Args[1]
	storage, err := CreateDuckDBStorage(filename)
	if err != nil {
		log.Panic(err)
	}

	// Set up web server
	app := fiber.New()

	// Request loggin
	app.Use(logger.New())

	app.Get("/", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	app.Get("/query", func(c *fiber.Ctx) error {
		query := c.Query("q")
		format := c.Query("format")

		user_api_key := c.Get("X-API-KEY")
		if user_api_key != api_key {
			return fiber.NewError(fiber.StatusUnauthorized, "Unauthorized")
		}

		// Execute query
		rows, err := storage.Query(query)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		defer rows.Close()

		if strings.EqualFold(format, "html") {
			return toHTML(rows, c)
		}

		return toJSON(rows, c)
	})

	app.Post("/data", func(c *fiber.Ctx) error {
		user_api_key := c.Get("X-API-KEY")
		if user_api_key != api_key {
			return fiber.NewError(fiber.StatusUnauthorized, "Unauthorized")
		}

		input := c.Body()

		// Ensure JSON is valid
		if !json.Valid(input) {
			return fiber.ErrBadRequest
		}

		// flat, err := flatten.FlattenString(string(input), "", flatten.DotStyle)
		// log.Println(flat)

		// root, err := ajson.Unmarshal([]byte(flat))
		root, err := ajson.Unmarshal(input)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		table_name := c.Get("X-SCRATCHDB-TABLE")
		data_path := "$"
		if table_name == "" {
			table, err := root.GetKey("table")
			if err != nil {
				return fiber.NewError(fiber.StatusBadRequest, err.Error())
			}
			table_name = table.String()
			data_path = "$.data"
		}

		// x, err := root.GetKey("data")
		x, err := root.JSONPath(data_path)
		if err != nil {
			return err
		}
		// log.Println(err)
		// log.Println(x[0].String())

		flat, err := flatten.FlattenString(x[0].String(), "", flatten.DotStyle)

		if os.Getenv("BATCH") == "1" {
			logfile.WriteString(table_name + "\t" + flat + "\n")
			return c.SendString("ok")
		}
		// logfile.Write(c.Body())
		// logfile.WriteString("\n")

		data_root, err := ajson.Unmarshal([]byte(flat))

		data, err := data_root.JSONPath("$")
		if err != nil {
			return err
		}

		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		err = storage.WriteJSONRow(table_name, data)
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		return c.SendString("ok")
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		_ = <-c
		fmt.Println("Gracefully shutting down...")

		// TODO: set readtimeout to something besides 0 to close keepalive connections
		_ = app.Shutdown()
	}()

	if os.Getenv("ENV") == "PROD" {
		runSSL(app)
	} else {
		if err := app.Listen(":3000"); err != nil {
			log.Panic(err)
		}
	}

	fmt.Println("Running cleanup tasks...")
	storage.Close()
	logfile.Close()
}
