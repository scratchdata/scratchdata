package memory

import (
	"time"

	"gorm.io/gorm"
)

type ShareLink struct {
	gorm.Model
	UUID          string `gorm:"index:idx_uuid,unique"`
	DestinationID int64
	Query         string
	ExpiresAt     time.Time
}
