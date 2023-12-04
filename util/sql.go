package util

import (
	"encoding/json"
	"strings"
)

// Trims whitespace and trailing ; characters from sql
func TrimQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	semi := strings.TrimSuffix(trimmed, ";")
	return semi
}

// Takes a string and returns a JSON-escaped version
func JsonEscape(i string) string {
	b, err := json.Marshal(i)
	if err != nil {
		panic(err)
	}
	// Trim the beginning and trailing " character
	return string(b[1 : len(b)-1])
}
