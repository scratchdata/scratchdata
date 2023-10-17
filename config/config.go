package config

import "scratchdb/ch"

type Config struct {
	Ingest     IngestConfig         `mapstructure:"ingest"`
	Insert     InsertConfig         `mapstructure:"insert"`
	AWS        AWS                  `mapstructure:"aws"`
	SSL        SSL                  `mapstructure:"ssl"`
	Storage    Storage              `mapstructure:"storage"`
	Clickhouse ch.ClickhouseServers `mapstructure:"clickhouse"`
	Users      map[string]string    `mapstructure:"users"`
}

type InsertConfig struct {
	Enabled                bool   `mapstructure:"enabled"` // Not used
	Workers                int    `mapstructure:"workers"`
	SleepSeconds           int    `mapstructure:"sleep_seconds"`
	DataDir                string `mapstructure:"data"`
	FreeSpaceRequiredBytes int64  `mapstructure:"free_space_required_bytes"`
}

type IngestConfig struct {
	Enabled bool   `mapstructure:"enabled"` // Not used
	Port    string `mapstructure:"port"`
	DataDir string `mapstructure:"data"`

	// How often to rotate log file
	MaxAgeSeconds int `mapstructure:"max_age_seconds"`

	// Max file size before rotating
	MaxSizeBytes int64 `mapstructure:"max_size_bytes"`

	HealthCheckPath        string `mapstructure:"health_check_path"`
	FreeSpaceRequiredBytes int64  `mapstructure:"free_space_required_bytes"`
}

type AWS struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	SQS             string `mapstructure:"sqs"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
}

type Storage struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
}

type SSL struct {
	Enabled   bool     `mapstructure:"enabled"`
	Hostnames []string `mapstructure:"hostnames"`
}
