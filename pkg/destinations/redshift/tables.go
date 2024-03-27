package redshift

import (
	"errors"

	"github.com/scratchdata/scratchdata/models"
)

func (b *RedshiftServer) Columns(table string) ([]models.Column, error) {
	return []models.Column{}, errors.New("not implemented")
}

func (b *RedshiftServer) Tables() ([]string, error) {
	return []string{}, errors.New("not implemented")
}
