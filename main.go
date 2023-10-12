package main

import (
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/spf13/viper"
	"scratchdb/config"
	"scratchdb/importer"
	"scratchdb/ingest"
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

	var wg sync.WaitGroup

	switch os.Args[1] {
	case "ingest":
		i := ingest.NewFileIngest(&C)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			fmt.Println("Gracefully shutting down import...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	case "insert":
		i := importer.NewImporter(&C)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			fmt.Println("Gracefully shutting down insert...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	default:
		log.Println("Expected ingest or insert")
		os.Exit(1)
	}

	wg.Wait()

}
