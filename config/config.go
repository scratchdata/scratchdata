package config

type Config struct {
	Ingest IngestConfig `mapstructure:"ingest"`
	AWS    AWS          `mapstructure:"aws"`
}

type IngestConfig struct {
	Port string `mapstructure:"port"`

	Data          string `mapstructure:"data"`
	MaxAgeSeconds int    `mapstructure:"max_age_seconds"`
	MaxSizeBytes  int64  `mapstructure:"max_size_bytes"`
}

type AWS struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	SQS             string `mapstructure:"sqs"`
	Region          string `mapstructure:"region"`
}
