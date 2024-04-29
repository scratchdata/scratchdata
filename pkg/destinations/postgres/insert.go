package postgres

import (
	"bufio"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/scratchdata/scratchdata/pkg/util"
	"github.com/tidwall/gjson"

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
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	const maxCapacity int = 50 * 1000000 // 50 MB line max
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	txn, err := s.conn.Begin()
	if err != nil {
		return err
	}

	var stmt *sql.Stmt

	recordsScanned := 0
	keys := make([]string, 0)
	var values []any

	for scanner.Scan() {
		line := gjson.ParseBytes(scanner.Bytes())

		if recordsScanned == 0 {
			keyJson := line.Get("@keys")
			for _, key := range keyJson.Array() {
				keys = append(keys, key.String())
			}

			values = make([]any, len(keys))

			stmt, err = txn.Prepare(pq.CopyInSchema(s.Schema, table, keys...))
			if err != nil {
				return err
			}
		}

		for i, key := range keys {
			values[i] = line.Get(key).String()
		}

		_, err = stmt.Exec(values...)
		if err != nil {
			return err
		}

		recordsScanned++
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
