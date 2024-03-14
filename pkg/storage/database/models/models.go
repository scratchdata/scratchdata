package models

type APIKey struct {
	ID            string `toml:"id"`
	DestinationID int64  `toml:"destination_id"`
}
