package models

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	JSONType string `json:"-"`
}
