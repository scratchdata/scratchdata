package redshift

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/s3"
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

func (s *RedshiftServer) InsertFromNDJsonFile(table string, fileName string) error {
	params := make(map[string]any)

	params["region"] = s.S3Region
	params["access_key_id"] = s.S3AccesKeyId
	params["secret_access_key"] = s.S3SecretAccessKey
	params["bucket"] = s.S3Bucket
	params["skipDefaultConfig"] = true

	s3Connection, err := s3.NewStorage(params)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to s3 storage")
		return err
	}
	absoluteFile, err := filepath.Abs(fileName)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to get absolute filepath")
		return err
	}

	file, err := os.Open(absoluteFile)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open file")
		return err
	}
	defer file.Close()

	// Upload the file to S3
	err = s3Connection.Upload(fileName, file)
	if err != nil {
		log.Fatal().Err(err).Str("Filename", fileName).Msg("Failed to upload file to s3")
		return err
	}

	copyCommand := fmt.Sprintf(
		"COPY %s FROM 's3://%s/%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' REGION '%s' FORMAT AS JSON 'auto';",
		table,
		s.S3Bucket,
		fileName,
		s.S3AccesKeyId,
		s.S3SecretAccessKey,
		s.S3Region,
	)
	_, err = s.conn.Exec(copyCommand)
	if err != nil {
		log.Fatal().Err(err).Str("Filename", fileName).Msg("Failed to copy file to redshift")
		return err
	}

	if s.DeleteFromS3 {
		err = s3Connection.Delete(fileName)
		if err != nil {
			log.Fatal().Err(err).Str("Filename", fileName).Msg("Failed to delete file from s3")
			return err
		}
	}
	return nil
}
