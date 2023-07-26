package ingest

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"scratchdb/config"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/jeremywohl/flatten"
	"github.com/spyzhov/ajson"
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

	// TODO: create one per api key. put this in the InsertData function, check a map[api_key]writer to see if it exists
	i.writers = make(map[string]*FileWriter)
	return i
}

func (i *FileIngest) Index(c *fiber.Ctx) error {
	return c.SendString("ok")
}

func (i *FileIngest) InsertData(c *fiber.Ctx) error {
	api_key := c.Get("X-API-KEY")
	// TODO: validate api key

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
	table_name := c.Get("X-SCRATCHDB-TABLE")
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

	flat, err := flatten.FlattenString(x[0].String(), "", flatten.UnderscoreStyle)
	if err != nil {
		return err
	}

	dir := filepath.Join(i.Config.Ingest.Data, api_key, table_name)
	writer, ok := i.writers[dir]
	if !ok {
		writer = NewFileWriter(dir, i.Config.Ingest.MaxAgeSeconds, i.Config.Ingest.MaxSizeBytes, i.Config.AWS)
		i.writers[dir] = writer
	}

	err = writer.Write(flat)
	if err != nil {
		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	}
	return c.SendString("ok")
}

func (i *FileIngest) Start() {
	// TODO: recover from non-graceful shutdown. What if there are files left on disk when we restart?

	i.app.Use(logger.New())

	i.app.Get("/", i.Index)
	i.app.Post("/data", i.InsertData)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		_ = <-c
		fmt.Println("Gracefully shutting down...")

		// TODO: set readtimeout to something besides 0 to close keepalive connections
		_ = i.app.Shutdown()
	}()

	if err := i.app.Listen(":" + i.Config.Ingest.Port); err != nil {
		log.Panic(err)
	}

	fmt.Println("Running cleanup tasks...")

	// Closing writers
	for name, writer := range i.writers {
		log.Println("Closing writer", name)
		err := writer.Close()
		if err != nil {
			log.Println(err)
		}
	}

}
