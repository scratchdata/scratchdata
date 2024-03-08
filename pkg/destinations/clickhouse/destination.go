package clickhouse

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"os"
)

func (s *ClickhouseServer) CreateEmptyTable(table string) error {
	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s"."%s" 
		(
		    __row_id Int64
		)
		 ENGINE = MergeTree
		PRIMARY KEY(__row_id)
	`, s.Database, table)

	return s.conn.Exec(context.TODO(), sql)
}

func (s *ClickhouseServer) CreateColumns(table string, filePath string) error {
	input, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer input.Close()

	columns, err := s.inferColumnTypes(input)
	if err != nil {
		log.Err(err).Msg("failed to retrieve columns from input JSON")
		return err
	}

	err = s.createColumnsWithTypes(table, columns)
	if err != nil {
		log.Err(err).Msg("failed to create columns")
		return err
	}

	return nil
}

func (s *ClickhouseServer) InsertFromNDJsonFile(table string, filePath string) error {
	input, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer input.Close()

	columns, err := s.inferColumnTypes(input)
	if err != nil {
		log.Err(err).Msg("failed to retrieve columns from input JSON")
		return err
	}

	err = s.insertData(input, table, columns)
	if err != nil {
		log.Err(err).Msg("Failed to insert data")
		return err
	}

	return nil
}
