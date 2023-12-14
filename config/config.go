package config

import (
	"fmt"
	"scratchdata/pkg/database"
	"scratchdata/pkg/filestore"
	"scratchdata/pkg/queue"
	"scratchdata/pkg/transport"
	"scratchdata/pkg/transport/memory"
	"scratchdata/pkg/transport/queuestorage"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/rs/zerolog"
)

type Config struct {
	QueueProviderName     string `toml:"queue_provider"`
	StorageProviderName   string `toml:"storage_provider"`
	TransportProviderName string `toml:"transport_provider"`

	AccountManager map[string]interface{} `toml:"account_manager"`

	Logs Logs `toml:"logs"`

	S3  S3  `toml:"s3"`
	SQS SQS `toml:"sqs"`
	API API `toml:"api"`

	Database database.Database

	Transport transport.DataTransport
}

type configData struct {
	Database  map[string]interface{} `toml:"database"`
	Transport toml.Primitive         `toml:"transport"`

	Config
}

type Logs struct {
	Pretty bool `toml:"pretty"`
	// panic, fatal, error, warn, info, debug, trace
	Level string `toml:"level"`
}

func (loggingConfig Logs) ToLevel() zerolog.Level {
	switch loggingConfig.Level {
	case "panic":
		return zerolog.PanicLevel
	case "fatal":
		return zerolog.FatalLevel
	case "error":
		return zerolog.ErrorLevel
	case "warn":
		return zerolog.WarnLevel
	case "info":
		return zerolog.InfoLevel
	case "debug":
		return zerolog.DebugLevel
	case "trace":
		return zerolog.TraceLevel
	}
	return zerolog.TraceLevel
}

type S3 struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
	Region    string `toml:"region"`
	Endpoint  string `toml:"endpoint"`
}

func (s S3) Validate() error {
	var invalid []string
	if s.AccessKey == "" {
		invalid = append(invalid, "access_key")
	}
	if s.SecretKey == "" {
		invalid = append(invalid, "secret_key")
	}
	if s.Bucket == "" {
		invalid = append(invalid, "bucket")
	}
	if s.Region == "" {
		invalid = append(invalid, "region")
	}
	if s.Endpoint == "" {
		invalid = append(invalid, "endpoint")
	}
	if len(invalid) == 0 {
		return nil
	}
	return fmt.Errorf("The [s3] section contains missing or invalid field(s): %s",
		strings.Join(invalid, ", "))
}

type SQS struct {
	AccessKey    string `toml:"access_key"`
	SecretAccess string `toml:"secret_key"`
	Region       string `toml:"region"`
	Endpoint     string `toml:"endpoint"`
}

func (s SQS) Validate() error {
	var invalid []string
	if s.AccessKey == "" {
		invalid = append(invalid, "access_key")
	}
	if s.SecretAccess == "" {
		invalid = append(invalid, "secret_key")
	}
	if s.Region == "" {
		invalid = append(invalid, "region")
	}
	if s.Endpoint == "" {
		invalid = append(invalid, "endpoint")
	}
	if len(invalid) == 0 {
		return nil
	}
	return fmt.Errorf("The [ssq] section contains missing or invalid field(s): %s",
		strings.Join(invalid, ", "))

}

type API struct {
	Enabled bool   `toml:"enabled"`
	Port    int    `toml:"port"`
	DataDir string `toml:"data"`

	// How often to rotate log file
	MaxAgeSeconds int `toml:"max_age_seconds"`

	// Max file size before rotating
	MaxSizeBytes int64 `toml:"max_size_bytes"`

	HealthCheckPath        string `toml:"health_check_path"`
	FreeSpaceRequiredBytes int64  `toml:"free_space_required_bytes"`
}

// decodeTransportMemory decodes the [transport] config for `[transport.type]` = `memory`
func decodeTransportMemory(metaData toml.MetaData, confData configData) (transport.DataTransport, error) {
	return memory.NewMemoryTransport(confData.Config.Database), nil
}

// decodeTransportQueueStorage decodes the [transport] config for `[transport.type]` = `queuestorage`
func decodeTransportQueueStorage(metaData toml.MetaData, confData configData) (transport.DataTransport, error) {
	fields := struct {
		Queue   string `toml:"queue"`
		Storage string `toml:"storage"`
	}{}
	if err := metaData.PrimitiveDecode(confData.Transport, &fields); err != nil {
		return nil, fmt.Errorf("Cannot decode transport.queue and/or transport.storage: %w", err)
	}

	var queueBackend queue.QueueBackend
	switch fields.Queue {
	case "sqs":
		if err := confData.SQS.Validate(); err != nil {
			return nil, fmt.Errorf("Cannot create SQS queue: %w", err)
		}
		// TODO: construct SQS instance
		queueBackend = nil
	default:
		return nil, fmt.Errorf("config.Load: Invalid transport.queue: %s; Expected %s",
			fields.Queue,
			"sqs",
		)
	}

	var storageBackend filestore.StorageBackend
	switch fields.Storage {
	case "s3":
		if err := confData.S3.Validate(); err != nil {
			return nil, fmt.Errorf("Cannot create S3 storage: %w", err)
		}
		// TODO: construct S3 instance
		storageBackend = nil
	default:
		return nil, fmt.Errorf("config.Load: Invalid transport.storage: %s; Expected %s",
			fields.Storage,
			"s3",
		)
	}

	// TODO: remove this when queueBackend and storageBackend are properly constructed
	if queueBackend == nil || storageBackend == nil {
		return memory.NewMemoryTransport(confData.Config.Database), nil
	}
	return queuestorage.NewQueueStorageTransport(queueBackend, storageBackend), nil
}

// decodeTransport decode the `[transport]` config
func decodeTransport(metaData toml.MetaData, confData configData) (transport.DataTransport, error) {
	fields := struct {
		Type string `toml:"type"`
	}{}
	if err := metaData.PrimitiveDecode(confData.Transport, &fields); err != nil {
		return nil, fmt.Errorf("Cannot decode transport.type: %w", err)
	}

	switch fields.Type {
	case "", "memory":
		return decodeTransportMemory(metaData, confData)
	case "queuestorage":
		return decodeTransportQueueStorage(metaData, confData)
	default:
		return nil, fmt.Errorf("config.Load: Unsupported transport.type: %s; Expected %s or %s",
			fields.Type,
			"memory",
			"queuestorage",
		)
	}
}

// Load reads and validates the config stored in filePath
func Load(filePath string) (Config, error) {
	var confData configData
	metaData, err := toml.DecodeFile(filePath, &confData)
	if err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}

	// must be initialized before transport
	confData.Config.Database = database.GetDB(confData.Database)

	confData.Config.Transport, err = decodeTransport(metaData, confData)
	if err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}

	// guard against invalid input e.g. `[transport.name]` ... where we expect `[transport.type]`
	if undecoded := metaData.Undecoded(); len(undecoded) != 0 {
		return Config{}, fmt.Errorf("config.Load: Config contains extraneous fields: %v", undecoded)
	}
	return confData.Config, nil
}
