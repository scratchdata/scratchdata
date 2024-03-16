package redshift

import (
	"path/filepath"

	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"

	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/s3"
)

func (s *RedshiftServer) getGolumnNames(table string) (map[string]bool, error) {
	schema := "public"
	if s.Schema != "" {
		schema = s.Schema
	}

	sql := `
		select "column" as column_name
		from pg_table_def
		where schemaname = $1 and tablename = $2
	`

	m := map[string]bool{}
	rows, err := s.conn.Query(sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("getGolumnNames: cannot fetch column names: %w", err)
	}
	defer rows.Close()
	for rows.Next() {

		name := ""
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("getGolumnNames: cannot scan column name: %w", err)
		}
		m[strings.ToLower(name)] = true
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("getGolumnNames: no columns found: %w", err)
	}
	return m, nil
}

func (s *RedshiftServer) createColumns(table string, jsonTypes map[string]string) error {
	cols, err := s.getGolumnNames(table)
	if err != nil {
		return err
	}

	log.Printf("Found existing columns %v", cols)

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

		if _, ok := cols[strings.ToLower(colName)]; !ok {
			log.Printf("Column %s does not exist, creating it", colName)
			sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s", table, colName, colType)
			_, err = s.conn.Exec(sql)
			if err != nil {
				return err
			}
		}

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

func (s *RedshiftServer) InsertFromNDJsonFile(table string, filePath string) error {
	// Make sure the table exists

	// Recalling createColumns to create columns in the table if missing,  will be created
	err := s.CreateColumns(table, filePath)
	if err != nil {
		return err

	}

	s3Client, err := s3.NewStorageWithCreds(s.S3AccessKeyId, s.S3SecretAccessKey, s.S3Bucket, s.S3Region)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to create blobstore")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	absoluteFile, err := filepath.Abs(filePath)
	if err != nil {
		return err
	}

	file, err = os.Open(absoluteFile)
	if err != nil {
		return err
	}
	defer file.Close()

	err = s3Client.Upload(filePath, file)
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	copyCommand := fmt.Sprintf("COPY %s FROM 's3://%s/%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' FORMAT AS JSON 'auto'", table, s.S3Bucket, filePath, s.S3AccessKeyId, s.S3SecretAccessKey)

	_, err = s.conn.Exec(copyCommand)
	if err != nil {
		return err
	}

	err = s3Client.Delete(filePath)
	if err != nil {
		return err
	}
	return nil
}
