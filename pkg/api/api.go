package api

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/jwtauth/v5"
	"github.com/jellydator/ttlcache/v3"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"golang.org/x/oauth2"

	"github.com/bwmarrin/snowflake"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/datasink"
	"github.com/scratchdata/scratchdata/pkg/destinations"
)

type ScratchDataAPIStruct struct {
	storageServices    *storage.Services
	destinationManager *destinations.DestinationManager
	dataSink           datasink.DataSink
	snow               *snowflake.Node
	googleOauthConfig  *oauth2.Config
	tokenAuth          *jwtauth.JWTAuth
	config             config.API
	apiKeyCache        *ttlcache.Cache[string, models.APIKey]
	apiKeyCacheEnabled bool
}

func NewScratchDataAPI(
	storageServices *storage.Services,
	destinationManager *destinations.DestinationManager,
	dataSink datasink.DataSink,
	conf config.ScratchDataConfig,
) (*ScratchDataAPIStruct, error) {
	snow, err := util.NewSnowflakeGenerator()
	if err != nil {
		return nil, err
	}

	privKey := []byte(conf.Crypto.JWTPrivateKey)
	block, _ := pem.Decode(privKey)
	if block == nil {
		return nil, fmt.Errorf("unable to decode PEM block")
	}
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	var (
		apiKeyCache        *ttlcache.Cache[string, models.APIKey]
		apiKeyCacheEnabled = true
	)
	if conf.API.APIKeyCacheTTL == 0 {
		apiKeyCacheEnabled = false
	} else {
		apiKeyCache = ttlcache.New[string, models.APIKey](
			ttlcache.WithTTL[string, models.APIKey](time.Duration(conf.API.APIKeyCacheTTL) * time.Second),
		)
		go apiKeyCache.Start()
	}

	return &ScratchDataAPIStruct{
		storageServices:    storageServices,
		destinationManager: destinationManager,
		dataSink:           dataSink,
		snow:               snow,
		config:             conf.API,
		tokenAuth:          jwtauth.New("RS256", privateKey, nil),
		googleOauthConfig: &oauth2.Config{
			RedirectURL:  conf.Dashboard.GoogleRedirectURL,
			ClientID:     conf.Dashboard.GoogleClientID,
			ClientSecret: conf.Dashboard.GoogleClientSecret,
			Scopes:       []string{"https://www.googleapis.com/auth/userinfo.email"},
			Endpoint: oauth2.Endpoint{
				AuthURL:  "https://api.supabase.com/v1/oauth/authorize",
				TokenURL: "https://api.supabase.com/v1/oauth/token",
			},
		},
		apiKeyCache:        apiKeyCache,
		apiKeyCacheEnabled: apiKeyCacheEnabled,
	}, nil
}

func RunAPI(ctx context.Context, config config.API, mux *chi.Mux) {
	log.Debug().Int("port", config.Port).Msg("Starting API")

	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", config.Port),
		Handler: mux,
	}

	serverCtx, serverStopCtx := context.WithCancel(context.Background())

	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Err(err).Msg("Error serving API")
			serverStopCtx()
		}
	}()

	go func() {
		<-ctx.Done() // Wait for the context to be canceled

		log.Debug().Msg("Stopping API")

		// Gracefully shutdown server
		shutdownCtx, cancel := context.WithTimeout(serverCtx, 30*time.Minute)
		err := server.Shutdown(shutdownCtx)
		if err != nil {
			log.Error().Err(err).Msg("Error shutting down API")
		}

		cancel()
		<-shutdownCtx.Done()

		serverStopCtx()
	}()

	<-serverCtx.Done()

	log.Debug().Msg("API server stopped")
}
