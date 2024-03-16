package redshift

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"
)

func (s *RedshiftServer) createColumns(table string, jsonTypes map[string]string) error {
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

		sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, colType)
		_, err := s.conn.Exec(sql)
		if err != nil {
			return err
		}

		return nil

	}

	return nil
}
func (s *RedshiftServer) CreateColumns(table string, fileName string) error {

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

func (s *RedshiftServer) CreateEmptyTable(table string) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id BIGINT)", table)
	_, err := s.conn.Exec(sql)
	return err
}

// func (s *RedshiftServer) insertFromLocal(table string, localPath string) error {
// 	sql := fmt.Sprintf(`
// 		INSERT INTO "%s"
// 		BY NAME
// 		SELECT * FROM
// 		read_ndjson_auto('%s')
// 		`,
// 		table, localPath,
// 	)

// 	log.Trace().Str("sql", sql).Msg("Insert data SQL")

// 	_, err := s.conn.Exec(sql)
// 	return err
// }

// func (s *RedshiftServer) InsertFromNDJsonFile(table string, fileName string) error {
// 	absoluteFile, err := filepath.Abs(fileName)
// 	if err != nil {
// 		return err
// 	}

// 	err = s.insertFromLocal(table, absoluteFile)
// 	return err
// }
