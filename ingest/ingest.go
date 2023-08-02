package ingest

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"scratchdb/config"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/gomarkdown/markdown"
	"github.com/gomarkdown/markdown/html"
	"github.com/gomarkdown/markdown/parser"
	"github.com/jeremywohl/flatten"
	"github.com/spyzhov/ajson"
	"golang.org/x/crypto/acme/autocert"
)

type FileIngest struct {
	Config config.Config

	app     *fiber.App
	writers map[string]*FileWriter
}

func NewFileIngest(config config.Config) FileIngest {
	i := FileIngest{
		Config: config,
	}
	i.app = fiber.New()

	i.writers = make(map[string]*FileWriter)
	return i
}

func (i *FileIngest) Index(c *fiber.Ctx) error {
	return c.SendString("ok")
}

// TODO: Common pool of writers and uploaders across all API keys, rather than one per API key
// TODO: Start the uploading process independent of whether new data has been inserted for that API key
func (i *FileIngest) InsertData(c *fiber.Ctx) error {
	api_key := utils.CopyString(c.Get("X-API-KEY", "NONE"))
	_, ok := i.Config.Users[api_key]
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	input := c.Body()

	// Ensure JSON is valid
	if !json.Valid(input) {
		return fiber.ErrBadRequest
	}

	root, err := ajson.Unmarshal(input)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	data_path := "$"
	table_name := utils.CopyString(c.Get("X-SCRATCHDB-TABLE"))
	if table_name == "" {
		table, err := root.GetKey("table")
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		table_name, err = table.GetString()
		if err != nil {
			return fiber.NewError(fiber.StatusBadRequest, err.Error())
		}

		data_path = "$.data"
	}

	x, err := root.JSONPath(data_path)
	if err != nil {
		return err
	}

	flat, err := flatten.FlattenString(x[0].String(), "", flatten.UnderscoreStyle)
	if err != nil {
		return err
	}

	dir := filepath.Join(i.Config.Ingest.DataDir, api_key, table_name)
	writer, ok := i.writers[dir]
	if !ok {
		writer = NewFileWriter(
			dir,
			i.Config.Ingest.MaxAgeSeconds,
			i.Config.Ingest.MaxSizeBytes,
			i.Config.AWS,
			filepath.Join("data", api_key, table_name),
			map[string]string{"api_key": api_key, "table_name": table_name},
		)
		i.writers[dir] = writer
	}

	err = writer.Write(flat)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	return c.SendString("ok")
}

func (im *FileIngest) query(database string, query string, format string) (*http.Response, error) {
	var ch_format string
	switch format {
	case "html":
		ch_format = "Markdown"
	case "json":
		ch_format = "JSONEachRow"
	default:
		ch_format = "JSONEachRow"
	}

	sql := "SELECT * FROM (" + query + ") FORMAT " + ch_format
	log.Println(sql)

	url := im.Config.Clickhouse.Protocol + "://" + im.Config.Clickhouse.Host + ":" + im.Config.Clickhouse.HTTPPort
	fmt.Println("URL:>", url)

	var jsonStr = []byte(sql)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-Clickhouse-User", im.Config.Clickhouse.Username)
	req.Header.Set("X-Clickhouse-Key", im.Config.Clickhouse.Password)
	req.Header.Set("X-Clickhouse-Database", database)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	return resp, nil
}

func (i *FileIngest) Query(c *fiber.Ctx) error {
	query := utils.CopyString(c.Query("q"))
	format := utils.CopyString(c.Query("format", "json"))
	api_key := utils.CopyString(c.Get("X-API-KEY", "NONE"))
	user, ok := i.Config.Users[api_key]
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	resp, err := i.query(user, query, format)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	defer resp.Body.Close()

	// log.Println(resp.Header)

	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return fiber.NewError(fiber.StatusBadRequest, string(msg))
	}

	firstRecord := true

	switch format {
	case "html":
		md, _ := io.ReadAll(resp.Body)
		// create markdown parser with extensions
		extensions := parser.CommonExtensions | parser.AutoHeadingIDs | parser.NoEmptyLineBeforeBlock
		p := parser.NewWithExtensions(extensions)
		doc := p.Parse(md)

		// create HTML renderer with extensions
		htmlFlags := html.CommonFlags | html.HrefTargetBlank
		opts := html.RendererOptions{Flags: htmlFlags}
		renderer := html.NewRenderer(opts)

		html := markdown.Render(doc, renderer)
		c.Set(fiber.HeaderContentType, fiber.MIMETextHTML)
		c.WriteString(`
		<style>
		table, tr, td, th {border: 1px solid; border-collapse:collapse}
		td,th{padding:3px;}
		</style>
		`)
		c.Write(html)
		return nil
	default:
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		c.WriteString("[")

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			if !firstRecord {
				c.WriteString(",")
			} else {
				firstRecord = false
			}

			c.Write(scanner.Bytes())
		}

		c.WriteString("]")
	}
	return nil
}

func (i *FileIngest) runSSL() {

	// Certificate manager
	m := &autocert.Manager{
		Prompt: autocert.AcceptTOS,
		// Replace with your domain
		HostPolicy: autocert.HostWhitelist(i.Config.SSL.Hostnames...),
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

	if err := i.app.Listener(ln); err != nil {
		log.Panic(err)
	}
}

func (i *FileIngest) Start() {
	// TODO: recover from non-graceful shutdown. What if there are files left on disk when we restart?

	i.app.Use(logger.New())

	i.app.Get("/", i.Index)
	i.app.Post("/data", i.InsertData)
	i.app.Get("/query", i.Query)

	if i.Config.SSL.Enabled {
		i.runSSL()
	} else {
		if err := i.app.Listen(":" + i.Config.Ingest.Port); err != nil {
			log.Panic(err)
		}
	}

}

func (i *FileIngest) Stop() error {
	fmt.Println("Running cleanup tasks...")

	// TODO: set readtimeout to something besides 0 to close keepalive connections
	err := i.app.Shutdown()
	if err != nil {
		log.Println(err)
	}

	// Closing writers
	for name, writer := range i.writers {
		log.Println("Closing writer", name)
		err := writer.Close()
		if err != nil {
			log.Println(err)
		}
	}

	return err
}
