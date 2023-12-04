package util

import "strings"

// Trims whitespace and trailing ; characters from sql
func TrimQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	semi := strings.TrimSuffix(trimmed, ";")
	return semi
}
