package models

type FileUploadMessage struct {
	APIKey string `json:"api_key"`
	Table  string `json:"table_name"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}
