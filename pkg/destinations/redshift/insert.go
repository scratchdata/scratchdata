package redshift

import (
	"fmt"
	"path/filepath"

	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/util"

	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/s3"
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

		sql := fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN \"%s\" %s", table, colName, colType)
		_, err := s.conn.Exec(sql)
		if err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				log.Err(err).Msg("createColumns: cannot create column")
				return err
			}

		}
		log.Info().Msgf("Created column %s %s", colName, colType)
		// if _, ok := cols[strings.ToLower(colName)]; !ok {

		// }

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

	params := make(map[string]any)

	params["region"] = s.S3Region
	params["access_key_id"] = s.S3AccessKeyId
	params["secret_access_key"] = s.S3SecretAccessKey
	params["bucket"] = s.S3Bucket
	params["skipDefaultConfig"] = true

	s3Client, err := s3.NewStorage(params)
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

	copyCommand := fmt.Sprintf("COPY %s FROM 's3://%s/%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' FORMAT AS JSON 'auto'", s.Schema+"."+table, s.S3Bucket, filePath, s.S3AccessKeyId, s.S3SecretAccessKey)

	_, err = s.conn.Exec(copyCommand)
	if err != nil {
		return err
	}
	if s.DeleteFromS3 {
		log.Info().Msgf("Deleting file %s from S3", filePath)
		err = s3Client.Delete(filePath)

		if err != nil {
			log.Err(err).Msg("Failed to delete file from S3")
			return err
		}
	}
	return nil
}
