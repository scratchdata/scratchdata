package view

import (
	"encoding/gob"
	"html/template"

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/util"
)

func init() {
	gob.Register(Flash{})
}

type Connections struct {
	Destinations []config.Destination
}

type UpsertConnection struct {
	RequestID   string
	Destination config.Destination
	TypeDisplay string
	FormFields  []util.Form
}

type Connect struct {
	APIKey string
	APIUrl string
}

type ShareQuery struct {
	Expires string
	Name    string
	ID      string
}

type FlashType string

const (
	FlashTypeSuccess FlashType = "success"
	FlashTypeWarning FlashType = "warning"
	FlashTypeError   FlashType = "error"
)

type Flash struct {
	Type    FlashType
	Title   string
	Message string
	Fatal   bool
}

type Request struct {
	URL string
}

type Model struct {
	CSRFToken        template.HTML
	Email            string
	HideSidebar      bool
	Flashes          []Flash
	Connect          Connect
	Connections      Connections
	UpsertConnection UpsertConnection
	Data             map[string]any
	Request          Request
	ShareQuery       ShareQuery
}
