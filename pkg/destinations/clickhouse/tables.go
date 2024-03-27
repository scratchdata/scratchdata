package clickhouse

import (
	"errors"

	"github.com/scratchdata/scratchdata/models"
)

func (b *ClickhouseServer) Columns(table string) ([]models.Column, error) {
	return []models.Column{}, errors.New("not implemented")
}

func (b *ClickhouseServer) Tables() ([]string, error) {
	return []string{}, errors.New("not implemented")
}
