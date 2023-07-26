package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"scratchdb/config"
	"scratchdb/ingest"

	"github.com/spf13/viper"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	ingestCmd := flag.NewFlagSet("ingest", flag.ExitOnError)
	ingestConfig := ingestCmd.String("config", "config.toml", "")
	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertConfig := insertCmd.String("config", "config.toml", "")

	var configFile string

	if len(os.Args) < 2 {
		fmt.Println("expected ingest or insert subcommands")
		os.Exit(1)
	}

	// Flag for server or consumer mode
	switch os.Args[1] {
	case "ingest":
		ingestCmd.Parse(os.Args[2:])
		configFile = *ingestConfig
	case "insert":
		insertCmd.Parse(os.Args[2:])
		configFile = *insertConfig
	default:
		log.Println("Expected ingest or insert")
		os.Exit(1)
	}

	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %w", err))
	}

	var C config.Config
	err = viper.Unmarshal(&C)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	switch os.Args[1] {
	case "ingest":
		i := ingest.NewFileIngest(C)
		i.Start()
	case "insert":
	default:
		log.Println("Expected ingest or insert")
		os.Exit(1)
	}

	// // Server mode
	// // Spin up web server
	// // When request comes in:
	// // - Check API key
	// // - Get table name
	// // - Append valid JSON to file: api_key/table_name/file.log
	// // The logger
	// // - appends data
	// // - rotates the log
	// // - uploads to s3
	// // - records in a database (dynamo, sqs)

	// if len(os.Args) < 3 {
	// 	log.Println("Must specify database file name and log file name")
	// 	os.Exit(1)
	// }

	// logfile, err := os.OpenFile(os.Args[2], os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Println(err)
	// 	os.Exit(1)
	// }

	// api_key := os.Getenv("API_KEY")

	// // Create DB connection
	// filename := os.Args[1]
	// storage, err := CreateDuckDBStorage(filename)
	// if err != nil {
	// 	log.Panic(err)
	// }

	// // Set up web server
	// app := fiber.New()

	// // Request loggin
	// app.Use(logger.New())

	// app.Get("/", func(c *fiber.Ctx) error {
	// 	return c.SendString("ok")
	// })

	// app.Get("/query", func(c *fiber.Ctx) error {
	// 	query := c.Query("q")
	// 	format := c.Query("format")

	// 	user_api_key := c.Get("X-API-KEY")
	// 	if user_api_key != api_key {
	// 		return fiber.NewError(fiber.StatusUnauthorized, "Unauthorized")
	// 	}

	// 	// Execute query
	// 	rows, err := storage.Query(query)
	// 	if err != nil {
	// 		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// 	}

	// 	defer rows.Close()

	// 	if strings.EqualFold(format, "html") {
	// 		return toHTML(rows, c)
	// 	}

	// 	return toJSON(rows, c)
	// })

	// app.Post("/data", func(c *fiber.Ctx) error {
	// 	user_api_key := c.Get("X-API-KEY")
	// 	if user_api_key != api_key {
	// 		return fiber.NewError(fiber.StatusUnauthorized, "Unauthorized")
	// 	}

	// 	input := c.Body()

	// 	// Ensure JSON is valid
	// 	if !json.Valid(input) {
	// 		return fiber.ErrBadRequest
	// 	}

	// 	// flat, err := flatten.FlattenString(string(input), "", flatten.DotStyle)
	// 	// log.Println(flat)

	// 	// root, err := ajson.Unmarshal([]byte(flat))
	// 	root, err := ajson.Unmarshal(input)
	// 	if err != nil {
	// 		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// 	}

	// 	table_name := c.Get("X-SCRATCHDB-TABLE")
	// 	data_path := "$"
	// 	if table_name == "" {
	// 		table, err := root.GetKey("table")
	// 		if err != nil {
	// 			return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// 		}
	// 		table_name = table.String()
	// 		data_path = "$.data"
	// 	}

	// 	// x, err := root.GetKey("data")
	// 	x, err := root.JSONPath(data_path)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	// log.Println(err)
	// 	// log.Println(x[0].String())

	// 	flat, err := flatten.FlattenString(x[0].String(), "", flatten.DotStyle)

	// 	if os.Getenv("BATCH") == "1" {
	// 		logfile.WriteString(table_name + "\t" + flat + "\n")
	// 		return c.SendString("ok")
	// 	}
	// 	// logfile.Write(c.Body())
	// 	// logfile.WriteString("\n")

	// 	data_root, err := ajson.Unmarshal([]byte(flat))

	// 	data, err := data_root.JSONPath("$")
	// 	if err != nil {
	// 		return err
	// 	}

	// 	if err != nil {
	// 		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// 	}

	// 	err = storage.WriteJSONRow(table_name, data)
	// 	if err != nil {
	// 		return fiber.NewError(fiber.StatusBadRequest, err.Error())
	// 	}

	// 	return c.SendString("ok")
	// })

	// c := make(chan os.Signal, 1)
	// signal.Notify(c, os.Interrupt)
	// go func() {
	// 	_ = <-c
	// 	fmt.Println("Gracefully shutting down...")

	// 	// TODO: set readtimeout to something besides 0 to close keepalive connections
	// 	_ = app.Shutdown()
	// }()

	// if os.Getenv("ENV") == "PROD" {
	// 	runSSL(app)
	// } else {
	// 	if err := app.Listen(":3000"); err != nil {
	// 		log.Panic(err)
	// 	}
	// }

	// fmt.Println("Running cleanup tasks...")
	// storage.Close()
	// logfile.Close()
}
