package config

type Logging struct {
	JSONFormat bool   `yaml:"json_format"`
	Level      string `yaml:"level"`
}

type API struct {
	Enabled bool `yaml:"enabled" env:"SCRATCH_API_ENABLED"`
	Port    int  `yaml:"port"`
	// DataDirectory          string
	// FreeSpaceRequiredBytes int64
	MaxAgeSeconds       int    `yaml:"max_age_seconds"`
	MaxSizeBytes        int64  `yaml:"max_size_bytes"`
	HealthCheckFailFile string `yaml:"healthcheck_fail_file"`
}

type Workers struct {
	Enabled                bool   `yaml:"enabled"  env:"SCRATCH_WORKERS_ENABLED"`
	Count                  int    `yaml:"count"`
	DataDirectory          string `yaml:"data_directory"`
	FreeSpaceRequiredBytes int64  `yaml:"free_space_required_bytes"`
}

type Queue struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type Cache struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type Database struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type BlobStore struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type Destination struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
	APIKeys  []string       `yaml:"api_keys"`
}

type DataSink struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type ScratchDataConfig struct {
	Logging      Logging       `yaml:"logging"`
	API          API           `yaml:"api"`
	Workers      Workers       `yaml:"workers"`
	DataSink     DataSink      `yaml:"data_sink"`
	Queue        Queue         `yaml:"queue"`
	Cache        Cache         `yaml:"cache"`
	Database     Database      `yaml:"database"`
	BlobStore    BlobStore     `yaml:"blob_store"`
	Destinations []Destination `yaml:"destinations"`
}

// type Database interface{}
// type Queue interface{}
// type Cache interface{}
// type BlobStore interface{}
// type DataSink interface{}

type StorageServices interface {
	Database() Database
	Queue() Queue
	Cache() Cache
	BlobStore() BlobStore
	DataSink() DataSink
}
