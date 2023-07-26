package config

type Config struct {
	Ingest IngestConfig `mapstructure:"ingest"`
}

type IngestConfig struct {
	Port string `mapstructure:"port"`

	Data          string `mapstructure:"data"`
	MaxAgeSeconds int    `mapstructure:"max_age_seconds"`
	MaxSizeBytes  int    `mapstructure:"max_size_bytes"`
}
