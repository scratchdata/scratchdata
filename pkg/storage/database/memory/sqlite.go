package memory

import (
	"time"

	"gorm.io/gorm"
)

type ShareLink struct {
	gorm.Model
	DestinationID int64
	Query         string
	ExpiresAt     time.Time
}
