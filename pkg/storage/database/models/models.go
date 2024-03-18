package models

import (
	"time"
)

type APIKey struct {
	ID            string `toml:"id"`
	DestinationID int64  `toml:"destination_id"`
}

type SharedQuery struct {
	ID            string
	Query         string
	DestinationID int64
	ExpiresAt     time.Time
}
