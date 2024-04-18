package models

import (
	"time"

	"github.com/scratchdata/scratchdata/pkg/config"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type SavedQuery struct {
	gorm.Model
	UUID          string `gorm:"index:idx_saved_query_uuid,unique"`
	DestinationID int64
	Name          string
	Query         string
	ExpiresAt     time.Time
	IsPublic      bool
	Slug          string
	APIKeyID      uint
	APIKey        APIKey
	QueryParams   datatypes.JSONMap
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
	Settings datatypes.JSONType[map[string]any]
}

func (d Destination) ToConfig() config.Destination {
	return config.Destination{
		ID:       int64(d.ID),
		Name:     d.Name,
		Type:     d.Type,
		Settings: d.Settings.Data(),
	}
}

type ConnectionRequest struct {
	gorm.Model
	RequestID     string `gorm:"index,unique"`
	DestinationID uint
	Destination   Destination
	Expiration    time.Time
}

type APIKeySavedQuery struct {
	gorm.Model
	SavedQueryID uint
	SavedQuery   SavedQuery
}

type APIKey struct {
	gorm.Model
	Name          string
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
