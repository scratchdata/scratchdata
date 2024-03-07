package models

type FileUploadMessage struct {
	DatabaseID int64  `json:"database_id"`
	Table      string `json:"table"`
	Key        string `json:"key"`
}
