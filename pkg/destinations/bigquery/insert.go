package bigquery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/scratchdata/scratchdata/pkg/util"

	"cloud.google.com/go/bigquery"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/gcs"
)

func (s *BigQueryServer) jsonTypeToBQType(jsonType string) bigquery.FieldType {
	switch jsonType {
	case "int":
		return bigquery.IntegerFieldType
	case "bool":
		return bigquery.BooleanFieldType
	case "float":
		return bigquery.FloatFieldType
	case "string":
		return bigquery.StringFieldType
	default:
		return bigquery.StringFieldType
	}
}

func (s *BigQueryServer) CreateEmptyTable(name string) error {
	ctx := context.Background()
	res := strings.Split(name, ".")
	if len(res) != 2 {
		log.Error().Str("table", name).Msg("CreateEmptyTable: table name should be in the format dataset.table")
		return fmt.Errorf("table name should be in the format dataset.table")
	}

	// first create the dataset if missing
	// using SDK, raw sql requires lot of other default params to be entered.
	meta := &bigquery.DatasetMetadata{
		Location: s.Location,
	}
	if err := s.conn.Dataset(res[0]).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "Error 409: Already Exists") {
		log.Error().Err(err).Str("dataset", res[0]).Msg("CreateEmptyTable: failed to create Dataset")
		return err
	}

	// does support BIGINT in raw SQL, this is alias for INT64 in bigquery
	// create table requires both dataset and table name
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (__row_id BIGINT)", name)
	_, err := s.conn.Query(query).Read(ctx)
	if err != nil {
		log.Error().Err(err).Str("query", query).Msg("CreateEmptyTable: failed to create Table")
		return err
	}

	return nil
}

func (s *BigQueryServer) createColumns(table string, jsonTypes map[string]string) error {
	ctx := context.Background()

	for colName, jsonType := range jsonTypes {
		colType := s.jsonTypeToBQType(jsonType)

		query := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `%s` %s", table, colName, colType)
		_, err := s.conn.Query(query).Read(ctx)
		if err != nil {
			log.Error().Err(err).Str("query", query).Msg("createColumns: cannot run query")
			return err
		}
		time.Sleep(2 * time.Second)
	}

	return nil
}

func (s *BigQueryServer) CreateColumns(table string, fileName string) error {
	input, err := os.Open(fileName)
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("CreateColumns: Unable to open file")
		return err
	}
	// Infer JSON types for the input
	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("CreateColumns: Unable to infer JSON types")
		return err
	}

	err = s.createColumns(table, jsonTypes)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("CreateColumns: Failed to create columns")
		return err
	}

	err = input.Close()
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("Unable to close file")
	}

	return nil
}

func (s *BigQueryServer) UploadAndStream(table string, filePath string) error {
	client, err := gcs.NewStorage(map[string]any{
		"bucket":           s.GCSBucketName,
		"credentials_json": s.CredentialsJsonString,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create GCS client")
		return err
	}

	gcsFilePath := ""
	if s.GCSFilePrefix != "" {
		gcsFilePath = s.GCSFilePrefix + "/"
	}
	gcsFilePath += table + "/" + filepath.Base(filePath)

	log.Info().Msg("Uploading file to GCS ")
	err = s.uploadFileToGCS(filePath, gcsFilePath, client)
	if err != nil {
		log.Error().Err(err).Str("file", filePath).Str("gcs_file", gcsFilePath).Msg("Failed to upload file to GCS")
		return err
	}
	log.Info().Str("gcs_file", gcsFilePath).Msg("Uploaded file to GCS")

	input, err := os.Open(filePath)
	if err != nil {
		log.Error().Err(err).Str("filename", filePath).Msg("Upload And Stream: Unable to open file")
		return err
	}
	// Infer JSON types for the input
	jsonTypes, err := util.GetJSONTypes(input)
	if err != nil {
		log.Error().Err(err).Str("filename", filePath).Msg("Upload And Stream: Unable to infer JSON types")
		return err
	}

	log.Info().Msg("Streaming data to BigQuery")
	err = s.streamDataToBigQuery(table, gcsFilePath, jsonTypes)
	if err != nil {
		log.Error().Err(err).Msg("Failed to stream data to BigQuery")
		return err
	}

	if s.DeleteFromGCS {
		log.Info().Msg("Deleting file from GCS")
		err = client.Delete(gcsFilePath)
		if err != nil {
			log.Error().Err(err).Str("gcs_file", gcsFilePath).Msg("Failed to delete file from GCS")
		} else {
			log.Info().Msg("Deleted file from GCS")
		}
	}

	return nil
}

func (s *BigQueryServer) uploadFileToGCS(filePath string, gcsFilePath string, client *gcs.Storage) error {

	file, err := os.Open(filePath)
	if err != nil {
		log.Error().Err(err).Str("file", filePath).Msg("Failed to open file")
		return err
	}

	err = client.Upload(gcsFilePath, file)
	if err != nil {
		log.Error().Err(err).Str("file", filePath).Str("gcs_file", gcsFilePath).Msg("Failed to upload file to GCS")
	}

	return nil
}

func (s *BigQueryServer) streamDataToBigQuery(table string, gcsFilePath string, jsonTypes map[string]string) error {

	location := fmt.Sprintf("gs://%s/%s", s.GCSBucketName, gcsFilePath)

	ctx := context.Background()

	columns := "("
	first := true
	for colName, jsonType := range jsonTypes {
		colType := s.jsonTypeToBQType(jsonType)
		if !first {
			columns += ", "
		} else {
			first = false
		}
		columns += fmt.Sprintf("`%s` %s", colName, colType)
	}

	columns += ")"

	query := fmt.Sprintf("LOAD DATA INTO %s %s FROM FILES ( format = 'JSON', uris = ['%s'] )", table, columns, location)
	_, err := s.conn.Query(query).Read(ctx)
	if err != nil {
		log.Error().Err(err).Str("query", query).Msg("StreamDataToBigQuery: failed to stream data to BigQuery")
		return err
	}

	log.Info().Msg("Successfully loaded data into BigQuery")

	return nil
}

func (s *BigQueryServer) InsertFromNDJsonFile(table string, filePath string) error {
	err := s.UploadAndStream(table, filePath)
	if err != nil {
		log.Error().Err(err).Str("table", table).Str("file", filePath).Msg("Failed to upload and stream data to BigQuery")
		return err
	}
	return nil
}
