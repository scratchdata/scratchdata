package models

import (
	"time"

	"gorm.io/gorm"
)

type SharedQuery struct {
	ID            string
	Query         string
	DestinationID int64
	ExpiresAt     time.Time
}

type ShareLink struct {
	gorm.Model
	UUID          string `gorm:"index:idx_uuid,unique"`
	DestinationID int64
	Query         string
	ExpiresAt     time.Time
}

type Team struct {
	gorm.Model
	Name string

	Users []*User `gorm:"many2many:user_team;"`
}

type User struct {
	gorm.Model

	Teams []*Team `gorm:"many2many:user_team;"`

	Email       string `gorm:"index:idx_email_authtype,unique"`
	AuthType    string `gorm:"index:idx_email_authtype,unique"`
	AuthDetails string
}

type Destination struct {
	gorm.Model
	TeamID   uint
	Team     Team
	Type     string
	Name     string
	Settings string
}

type APIKey struct {
	gorm.Model
	DestinationID uint
	Destination   Destination `gorm:"constraint:OnDelete:CASCADE"`
	HashedAPIKey  string      `gorm:"index"`
}

type MessageType string

const InsertData MessageType = "INSERT_DATA"
const CopyData MessageType = "COPY_DATA"

type MessageStatus string

const New MessageStatus = "NEW"
const Claimed MessageStatus = "CLAIMED"

type Message struct {
	gorm.Model
	MessageType MessageType   `gorm:"index"`
	Status      MessageStatus `gorm:"index"`
	ClaimedAt   time.Time
	ClaimedBy   string
	Message     string

	// For future pause/unpause
	DestinationID    uint   `gorm:"index"`
	DestinationTable string `gorm:"index"`
}
