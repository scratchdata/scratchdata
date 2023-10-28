package ingest

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"scratchdb/client"
	"scratchdb/config"
	"scratchdb/util"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/oklog/ulid/v2"
	"github.com/spyzhov/ajson"
	"golang.org/x/crypto/acme/autocert"
)

type FileIngest struct {
	Config *config.Config

	app     *fiber.App
	writers map[string]*FileWriter
}

func NewFileIngest(config *config.Config) FileIngest {
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

func (i *FileIngest) HealthCheck(c *fiber.Ctx) error {
	// Check if server has been manually marked as unhealthy
	_, err := os.Stat(i.Config.Ingest.HealthCheckPath)
	if !os.IsNotExist(err) {
		return fiber.ErrBadGateway
	}

	// Ensure we haven't filled up disk
	currentFreeSpace := util.FreeDiskSpace(i.Config.Ingest.DataDir)
	if currentFreeSpace <= uint64(i.Config.Ingest.FreeSpaceRequiredBytes) {
		log.Println("Out of disk, failing health check")
		return fiber.ErrBadGateway
	}

	return c.SendString("ok")
}

func (i *FileIngest) getField(header string, query string, body string, c *fiber.Ctx) (string, string) {
	// First try to get value from header
	rc := utils.CopyString(c.Get(header))
	location := "header"

	// Then try to get if from query param
	if rc == "" {
		rc = utils.CopyString(c.Query(query))
		location = "query"
	}

	// Then try to get it from JSON body
	if body != "" && rc == "" {
		location = "body"
		root, err := ajson.Unmarshal(c.Body())
		if err != nil {
			return "", ""
		}

		bodyKey, err := root.GetKey(body)
		rc, _ = bodyKey.GetString()
	}

	if rc == "" {
		return "", ""
	}
	return rc, location
}

// TODO: Common pool of writers and uploaders across all API keys, rather than one per API key
// TODO: Start the uploading process independent of whether new data has been inserted for that API key
func (i *FileIngest) InsertData(c *fiber.Ctx) error {
	rid := ulid.Make().String()
	if c.QueryBool("debug", false) {
		log.Println(rid, "Headers", c.GetReqHeaders())
		log.Println(rid, "Body", string(c.Body()))
		log.Println(rid, "Query Params", c.Queries())
	}

	apiKey, _ := i.getField("X-API-KEY", "api_key", "api_key", c)
	_, ok := i.Config.Users[apiKey]
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	tableName, tableLocation := i.getField("X-SCRATCHDB-TABLE", "table", "table", c)
	if tableName == "" {
		return fiber.NewError(fiber.StatusBadRequest, "You must specify a table name")
	}
	dir := filepath.Join(i.Config.Ingest.DataDir, apiKey, tableName)
	uploadDirectory := filepath.Join("data", apiKey, tableName)
	msgData := map[string]string{
		"api_key":    apiKey,
		"table_name": tableName,
		"bucket":     i.Config.AWS.S3Bucket,
	}

	format, _ := i.getField(fiber.HeaderContentType, "format", "", c)
	doUpload := func(ext string) error {
		svcClient := client.NewClient(i.Config)
		key := filepath.Join(uploadDirectory, rid+ext)
		msgData["format"] = ext[1:]
		msgData["key"] = filepath.Join(uploadDirectory, rid+ext)
		return handleFileUpload(
			svcClient,
			bytes.NewReader(c.Body()), i.Config.AWS.S3Bucket, key,
			i.Config.AWS.SQS, msgData)
	}
	switch format {
	case "parquet", "application/parquet", "application/x-parquet":
		// TODO: Ensure body have valid type
		err := doUpload(".parquet")
		if err != nil {
			log.Printf("failed to handle parquet: %s\n", err)
			return err
		}

	case "ndjson", "application/ndjson", "application/x-ndjson":
		err := doUpload(".ndjson")
		if err != nil {
			log.Printf("failed to handle ndjson: %s\n", err)
			return err
		}

	case "json", "application/json":
		fallthrough
	default:
		flattenAlgorithm, _ := i.getField("X-SCRATCHDB-FLATTEN", "flatten", "flatten", c)

		dataPath := "$"
		if tableLocation == "body" {
			dataPath = "$.data"
		}

		// TODO: make sure this is atomic!
		writer, ok := i.writers[dir]
		if !ok {
			writer = NewFileWriter(
				dir,
				i.Config,
				uploadDirectory,
				map[string]string{"api_key": apiKey, "table_name": tableName},
			)
			i.writers[dir] = writer
		}
		err := handleJSONUpload(c.Body(), writer, dataPath, flattenAlgorithm)
		if err != nil {
			return err
		}
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

	// Possibly use squirrel library here: https://github.com/Masterminds/squirrel
	sql := "SELECT * FROM (" + query + ") FORMAT " + ch_format
	// log.Println(sql)

	url := im.Config.Clickhouse.Protocol + "://" + im.Config.Clickhouse.Host + ":" + im.Config.Clickhouse.HTTPPort

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

	if c.Method() == "POST" {
		payload := struct {
			Query string `json:"query"`
		}{}

		if err := c.BodyParser(&payload); err != nil {
			return err
		}

		query = payload.Query
	}

	format := utils.CopyString(c.Query("format", "json"))
	api_key, _ := i.getField("X-API-KEY", "api_key", "", c)
	user, ok := i.Config.Users[api_key]
	if !ok {
		return fiber.NewError(fiber.StatusUnauthorized)
	}

	resp, err := i.query(user, query, format)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return fiber.NewError(fiber.StatusBadRequest, string(msg))
	}

	switch format {
	case "html":
		err = writeHTML(resp.Body, c)
	case "json":
		fallthrough
	default:
		err = writeJSON(resp.Body, c)
	}
	return err
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
	i.app.Get("/healthcheck", i.HealthCheck)
	i.app.Post("/data", i.InsertData)
	i.app.Get("/query", i.Query)
	i.app.Post("/query", i.Query)

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
