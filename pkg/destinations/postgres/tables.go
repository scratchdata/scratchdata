package postgres

import (
	"errors"

	"github.com/scratchdata/scratchdata/models"
)

func (b *PostgresServer) Columns(table string) ([]models.Column, error) {
	return []models.Column{}, errors.New("not implemented")
}

func (b *PostgresServer) Tables() ([]string, error) {
	return []string{}, errors.New("not implemented")
}
