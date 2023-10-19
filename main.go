package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"

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
		wg             sync.WaitGroup
		ctx, cancelCtx = context.WithCancel(context.Background())
	)
	defer cancelCtx()

	if ingestMode {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := ingest.NewFileIngest(ctx, &cfg)
			i.Start()
		}()
	}

	if insertMode {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := importer.NewImporter(ctx, &cfg, &cfg.Clickhouse)
			go i.Start()
		}()
	}

	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		// block until interrupt/terminate signal
		<-quitChannel
		cancelCtx()
		log.Println("gracefully shutting down")
	}()

	wg.Wait()
}
