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

	var (
		ingestMode, insertMode bool
		configFile             string
	)

	flag.BoolVar(&ingestMode, "ingest", false, "Run ingestion")
	flag.BoolVar(&insertMode, "insert", false, "Run imports")
	flag.StringVar(&configFile, "config", "config.toml", "Path to configuration file")
	flag.Parse()

	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(fmt.Errorf("fatal error config file: %w", err))
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	var (
		wg sync.WaitGroup
		c  = make(chan os.Signal, 1)
	)
	signal.Notify(c, os.Interrupt)

	if ingestMode && insertMode {
		log.Fatal("ingest and insert are mutually exclusive")
	} else if ingestMode {
		i := ingest.NewFileIngest(&cfg)

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = <-c
			log.Println("gracefully shutting down ingest")
			if err := i.Stop(); err != nil {
				log.Println(err)
			}
		}()
		i.Start()
	} else if insertMode {
		i := importer.NewImporter(&cfg)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = <-c
			log.Println("gracefully shutting down import")
			if err := i.Stop(); err != nil {
				log.Println(err)
			}
		}()
		i.Start()
	} else {
		log.Fatal("expected ingest or insert")
	}

	wg.Wait()
}
