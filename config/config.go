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

type Vault struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type BlobStore struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type Destination struct {
	ID       int64          `yaml:"id" json:"id"`
	Type     string         `yaml:"type" json:"type"`
	Name     string         `yaml:"name" json:"name"`
	Settings map[string]any `yaml:"settings" json:"settings"`
	APIKeys  []string       `yaml:"api_keys" json:"api_keys"`
}

type DataSink struct {
	Type     string         `yaml:"type"`
	Settings map[string]any `yaml:"settings"`
}

type CryptoConfig struct {
	JWTPrivateKey string `yaml:"jwt_private_key"`
}

type APIKey struct {
	Key string `yaml:"key"`
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
	APIKeys      []APIKey      `yaml:"api_keys"`

	Crypto CryptoConfig `yaml:"crypto"`
}
