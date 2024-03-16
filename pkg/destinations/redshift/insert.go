package redshift

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/config"
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
	var configOptions config.ScratchDataConfig
	err := cleanenv.ReadConfig(os.Args[1], &configOptions)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to read configuration file")
	}

	s3Connection, err := s3.NewStorage(configOptions.BlobStore.Settings)
	if err != nil {
		log.Fatal().Err(err).Msg("Unable to create blobstore")
	}
	absoluteFile, err := filepath.Abs(fileName)
	if err != nil {
		return err
	}

	file, err := os.Open(absoluteFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Upload the file to S3
	err = s3Connection.Upload(fileName, file)
	if err != nil {
		return err
	}

	type s3ConfigMap struct {
		s3_region            string
		s3_access_key_id     string
		s3_secret_access_key string
		s3_bucket            string
	}

	s3Config := util.ConfigToStruct[s3ConfigMap](configOptions.BlobStore.Settings)

	copyCommand := fmt.Sprintf(
		"COPY %s FROM 's3://%s/%s' CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' REGION '%s' FORMAT AS JSON 'auto';",
		table,
		s3Config.s3_bucket,
		s3Config.s3_secret_access_key,
		s3Config.s3_access_key_id,
		s3Config.s3_secret_access_key,
		s3Config.s3_region,
	)
	_, err = s.conn.Exec(copyCommand)
	if err != nil {
		return err
	}

	// if configOptions.Database.Settings.delete_from_s3 == true {
	// 	err = storage.Delete(fileName)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

}
