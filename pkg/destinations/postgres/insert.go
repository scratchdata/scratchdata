package postgres

import (
	"errors"
	"fmt"

	"github.com/scratchdata/scratchdata/pkg/util"

	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

func (s *PostgresServer) createColumns(table string, jsonTypes map[string]string) error {

	for colName, jsonType := range jsonTypes {
		colType := "VARCHAR"
		switch jsonType {
		case "int":
			colType = "BIGINT"
		case "bool":
			colType = "BOOLEAN"
		case "float":
			colType = "DOUBLE PRECISION"
		case "string":
			colType = "VARCHAR"
		default:
			colType = "VARCHAR"
		}

		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s", s.Schema+"."+table, colName, colType)
		_, err := s.conn.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				log.Error().Err(err).Str("column", colName).Msg("createColumns: cannot create column")
				return err
			}

		}
		log.Trace().Str("name", colName).Str("type", colType).Msg("Created column")

	}

	return nil
}
func (s *PostgresServer) CreateColumns(table string, fileName string) error {

	input, err := os.Open(fileName)
	if err != nil {
		return err
	}
	// Infer JSON types for the input
	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}

	err = s.createColumns(table, jsonTypes)
	if err != nil {
		return err
	}

	err = input.Close()
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("Unable to close file")
	}

	return nil

}

func (s *PostgresServer) CreateEmptyTable(table string) error {

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id BIGINT)", table)
	_, err := s.conn.Exec(sql)
	return err
}

func (s *PostgresServer) InsertFromNDJsonFile(table string, filePath string) error {
	return errors.New("not implemented")
}
