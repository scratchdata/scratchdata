package main

import (
	"flag"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"scratchdb/apikeys"
	"scratchdb/chooser"
	"scratchdb/config"
	"scratchdb/importer"
	"scratchdb/ingest"
	"scratchdb/servers"
	"scratchdb/users"

	"github.com/spf13/viper"
)

func main() {
	log.Logger = log.With().Caller().Logger()

	ingestCmd := flag.NewFlagSet("ingest", flag.ExitOnError)
	ingestConfig := ingestCmd.String("config", "config.toml", "")

	insertCmd := flag.NewFlagSet("insert", flag.ExitOnError)
	insertConfig := insertCmd.String("config", "config.toml", "")

	addUserCmd := flag.NewFlagSet("adduser", flag.ExitOnError)
	addUserName := addUserCmd.String("user", "", "")
	addUserConfig := addUserCmd.String("config", "config.toml", "")

	var configFile string

	if len(os.Args) < 2 {
		log.Fatal().Msg("Expected ingest or insert subcommands")
	}

	// Flag for server or consumer mode
	switch os.Args[1] {
	case "ingest":
		ingestCmd.Parse(os.Args[2:])
		configFile = *ingestConfig
	case "insert":
		insertCmd.Parse(os.Args[2:])
		configFile = *insertConfig
	case "adduser":
		addUserCmd.Parse(os.Args[2:])
		configFile = *addUserConfig
	default:
		log.Fatal().Msg("Expected ingest or insert")
	}

	viper.SetConfigFile(configFile)

	err := viper.ReadInConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("fatal error config file")
	}

	var C config.Config
	err = viper.Unmarshal(&C)
	if err != nil {
		log.Fatal().Err(err).Msg("unable to decode into struct")
	}

	if C.Logs.Pretty {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Caller().Logger()
	}
	zerolog.SetGlobalLevel(C.Logs.ToLevel())

	var wg sync.WaitGroup

	var apiKeyManager apikeys.APIKeys
	apiKeyManager = &apikeys.APIKeysFromConfig{
		Users: C.Users,
	}

	// var serverManager servers.ClickhouseManager
	// serverManager = servers.NewDefaultServerManager(C.ClickhouseServers)

	var serverManager servers.DatabaseServerManager
	serverManager = servers.NewDefaultServerManager()

	var serverChooser chooser.ServerChooser
	serverChooser = &chooser.DefaultChooser{}

	switch os.Args[1] {
	case "ingest":
		i := ingest.NewFileIngest(&C, apiKeyManager, serverManager, serverChooser)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			log.Info().Msg("Gracefully shutting down import...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	case "insert":
		i := importer.NewImporter(&C, apiKeyManager, serverManager, serverChooser)

		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)

		wg.Add(1)
		go func() {
			_ = <-c
			log.Info().Msg("Gracefully shutting down insert...")
			_ = i.Stop()
			wg.Done()
		}()

		i.Start()
	case "adduser":
		var userManager users.UserManager
		userManager = users.NewDefaultUserManager(serverManager)

		err := userManager.AddUser(*addUserName)
		if err != nil {
			log.Fatal().Err(err).Send()
		}
	default:
		log.Fatal().Msg("Expected ingest or insert")
	}

	wg.Wait()

}
