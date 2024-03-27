package duckdb

import (
	"errors"

	"github.com/scratchdata/scratchdata/models"
)

func (b *DuckDBServer) Columns(table string) ([]models.Column, error) {
	return []models.Column{}, errors.New("not implemented")
}

func (b *DuckDBServer) Tables() ([]string, error) {
	return []string{}, errors.New("not implemented")
}
