package bigquery

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/bigquery"

	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/gcs"
	"github.com/scratchdata/scratchdata/util"
)

func (s *BigQueryServer) CreateEmptyTable(name string) error {
	ctx := context.Background()

	datasetRef := s.conn.Dataset(s.DatasetID)

	tableRef := datasetRef.Table(name)

	schema := bigquery.Schema{
		// even though docs say it has BIGINT it shows not supported so going with INTEGER
		{Name: "__row_id", Type: bigquery.IntegerFieldType},
	}

	err := tableRef.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "googleapi: Error 409: Already Exists") {
			log.Error().Err(err).Str("table", name).Msg("CreateEmptyTable: cannot create table")
			return err
		}
	}

	return nil
}

func (s *BigQueryServer) createColumns(table string, jsonTypes map[string]string) error {
	ctx := context.Background()

	for colName, jsonType := range jsonTypes {
		colType := bigquery.StringFieldType
		switch jsonType {
		case "int":
			colType = bigquery.IntegerFieldType
		case "bool":
			colType = bigquery.BooleanFieldType
		case "float":
			colType = bigquery.FloatFieldType
		case "string":
			colType = bigquery.StringFieldType
		default:
			colType = bigquery.StringFieldType
		}

		query := fmt.Sprintf("ALTER TABLE `%s.%s` ADD COLUMN `%s` %s", s.DatasetID, table, colName, colType)
		_, err := s.conn.Query(query).Read(ctx)
		if err != nil && !strings.Contains(err.Error(), "Error 400: Column already exists") {
			log.Error().Err(err).Str("query", query).Msg("createColumns: cannot run query")
			return err
		}
	}

	return nil
}

func (s *BigQueryServer) CreateColumns(table string, fileName string) error {
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

func (s *BigQueryServer) getAllColumns(table string) ([]string, error) {
	ctx := context.Background()

	datasetRef := s.conn.Dataset(s.DatasetID)

	tableRef := datasetRef.Table(table)

	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0)
	for _, field := range meta.Schema {
		columns = append(columns, field.Name)
	}

	return columns, nil
}

func (s *BigQueryServer) ConvertNDJSONToCSVAndUpload(table string, filePath string, csvFilePath string) error {

	columns, err := s.getAllColumns(table)
	if err != nil {
		log.Error().Err(err).Str("table", table).Msg("Failed to get columns")
		return err
	}

	ndjsonFile, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer ndjsonFile.Close()

	csvFile, err := os.Create(csvFilePath)
	if err != nil {
		return err
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	csvWriter.Write(columns)

	jsonDecoder := json.NewDecoder(ndjsonFile)
	jsonDecoder.UseNumber() // to avoid float64 conversion
	for jsonDecoder.More() {

		var data map[string]interface{}
		err := jsonDecoder.Decode(&data)
		if err != nil {
			log.Error().Err(err).Msg("Failed to parse JSON object")
			continue
		}

		var csvRow []string
		for _, columnName := range columns {
			val, ok := data[columnName]
			if !ok {
				val = "" // for null
			}
			csvRow = append(csvRow, fmt.Sprintf("%v", val))
		}
		csvWriter.Write(csvRow)

	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}

	gcsFilePath := ""
	if s.GCSFilePrefix != "" {
		gcsFilePath = s.GCSFilePrefix + "/"
	}
	gcsFilePath += table + "/" + filepath.Base(csvFilePath)

	client, err := gcs.NewStorage(map[string]any{
		"bucket":           s.GCSBucketName,
		"credentials_json": s.CredentialsJsonString,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create GCS client")
		return err
	}

	err = s.uploadFileToGCS(table, csvFilePath, gcsFilePath, client)
	if err != nil {
		return err
	}

	log.Printf("Uploaded file to GCS")

	err = s.streamDataToBigQuery(table, gcsFilePath)
	if err != nil {
		return err
	}

	if s.DeleteFromGCS {
		err = client.Delete(gcsFilePath)
		if err != nil {

			log.Error().Err(err).Str("gcs_file", gcsFilePath).Msg("Failed to delete csv file from GCS")
		}

	}

	return nil
}

func (s *BigQueryServer) uploadFileToGCS(table string, filePath string, gcsFilePath string, client *gcs.Storage) error {

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	err = client.Upload(gcsFilePath, file)
	if err != nil {
		log.Error().Err(err).Str("file", filePath).Str("gcs_file", gcsFilePath).Msg("Failed to upload csv file to GCS")
	}
	// even if error hits, we should close and delete the file
	file.Close()

	err = os.Remove(filePath)
	if err != nil {
		log.Error().Err(err).Str("file", filePath).Msg("Failed to remove csv file")
	}

	return nil
}

func (s *BigQueryServer) streamDataToBigQuery(table string, csvFilePath string) error {
	ctx := context.TODO()

	dataset := s.conn.Dataset(s.DatasetID)
	tableRef := dataset.Table(table)

	location := fmt.Sprintf("gs://%s/%s", s.GCSBucketName, csvFilePath)
	gcsRef := bigquery.NewGCSReference(location)
	gcsRef.SkipLeadingRows = 1

	loader := tableRef.LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to run BigQuery loader job")
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to wait for BigQuery loader job")
		return err
	}

	if status.Err() != nil {
		log.Error().Err(status.Err()).Msg("Failed to load data into BigQuery")
		return status.Err()
	}
	log.Info().Msg("Successfully loaded data into BigQuery")

	return nil
}

func (s *BigQueryServer) InsertFromNDJsonFile(table string, filePath string) error {
	log.Printf("Generating csv file ... ")
	csvFilePath := strings.TrimSuffix(filePath, ".ndjson") + ".csv"
	err := s.ConvertNDJSONToCSVAndUpload(table, filePath, csvFilePath)
	if err != nil {
		return err
	}
	return nil
}
