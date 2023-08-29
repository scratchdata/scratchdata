package config

type Config struct {
	Ingest     IngestConfig      `mapstructure:"ingest"`
	Insert     InsertConfig      `mapstructure:"insert"`
	AWS        AWS               `mapstructure:"aws"`
	SSL        SSL               `mapstructure:"ssl"`
	Clickhouse ClickhouseConfig  `mapstructure:"clickhouse"`
	Users      map[string]string `mapstructure:"users"`
}

type ClickhouseConfig struct {
	Protocol string `mapstructure:"protocol"`
	Host     string `mapstructure:"host"`
	HTTPPort string `mapstructure:"http_port"`
	TCPPort  string `mapstructure:"tcp_port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type InsertConfig struct {
	Enabled      bool   `mapstructure:"enabled"` // Not used
	Workers      int    `mapstructure:"workers"`
	SleepSeconds int    `mapstructure:"sleep_seconds"`
	DataDir      string `mapstructure:"data"`
}

type IngestConfig struct {
	Enabled         bool   `mapstructure:"enabled"` // Not used
	Port            string `mapstructure:"port"`
	DataDir         string `mapstructure:"data"`
	MaxAgeSeconds   int    `mapstructure:"max_age_seconds"`
	MaxSizeBytes    int64  `mapstructure:"max_size_bytes"`
	HealthCheckPath string `mapstructure:"health_check_path"`
}

type AWS struct {
	AccessKeyId     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	S3Bucket        string `mapstructure:"s3_bucket"`
	SQS             string `mapstructure:"sqs"`
	Region          string `mapstructure:"region"`
	Endpoint        string `mapstructure:"endpoint"`
}

type SSL struct {
	Enabled   bool     `mapstructure:"enabled"`
	Hostnames []string `mapstructure:"hostnames"`
}
