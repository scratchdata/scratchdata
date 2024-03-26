package duckdb

import (
	"fmt"
	"github.com/scratchdata/scratchdata/pkg/util"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
)

func (s *DuckDBServer) createColumns(table string, jsonTypes map[string]string) error {
	for colName, jsonType := range jsonTypes {

		// TODO: Should we specify defaults, or use null as default?
		sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN IF NOT EXISTS \"%s\" %s", table, colName, jsonToDuck[jsonType])
		_, err := s.db.Exec(sql)
		if err != nil {
			return err
		}

		// sql = fmt.Sprintf("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", table, colName)
	}

	return nil
}

func (s *DuckDBServer) describeTable(table string) ([]string, map[string]string, error) {
	duckColumns := []string{}
	duckdbColTypes := make(map[string]string)

	sql := fmt.Sprintf("DESCRIBE \"%s\"", table)
	rows, err := s.db.Query(sql)
	if err != nil {
		return duckColumns, duckdbColTypes, err
	}

	for rows.Next() {
		var colName, colType string
		var isNull, key, defaultValue, extra *string
		if err := rows.Scan(&colName, &colType, &isNull, &key, &defaultValue, &extra); err != nil {
			return duckColumns, duckdbColTypes, err
		}

		duckdbColTypes[colName] = colType
		duckColumns = append(duckColumns, colName)
	}

	if err := rows.Err(); err != nil {
		return duckColumns, duckdbColTypes, err
	}

	return duckColumns, duckdbColTypes, err
}

func (s *DuckDBServer) insertFromLocal(table string, localPath string) error {
	sql := fmt.Sprintf(`
		INSERT INTO "%s" 
		BY NAME
		SELECT * FROM
		read_ndjson_auto('%s')
		`,
		table, localPath,
	)

	log.Trace().Str("sql", sql).Msg("Insert data SQL")

	_, err := s.db.Exec(sql)
	return err
}

func (s *DuckDBServer) CreateEmptyTable(table string) error {
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (__row_id BIGINT)", table)
	_, err := s.db.Exec(sql)
	return err
}

func (s *DuckDBServer) CreateColumns(table string, fileName string) error {
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

func (s *DuckDBServer) InsertFromNDJsonFile(table string, fileName string) error {
	absoluteFile, err := filepath.Abs(fileName)
	if err != nil {
		return err
	}

	err = s.insertFromLocal(table, absoluteFile)
	return err
}
