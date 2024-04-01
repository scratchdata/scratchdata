package models

type FileUploadMessage struct {
	DatabaseID int64  `json:"database_id"`
	Table      string `json:"table"`
	Key        string `json:"key"`
}

type CopyDataMessage struct {
	SourceID         int64  `json:"source_id"`
	Query            string `json:"query"`
	DestinationID    uint   `json:"destination_id"`
	DestinationTable string `json:"destination_table"`
}
