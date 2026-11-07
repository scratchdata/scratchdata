package bigquery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/storage/blobstore/gcs"
	"github.com/scratchdata/scratchdata/pkg/util"
)

// bqStreamLoadRequest encapsulates the GCS location, target table, and resolved column schema for BigQuery LOAD DATA.
type bqStreamLoadRequest struct {
	table       string
	gcsFilePath string
	colTypes    map[string]bigquery.FieldType
}

// gcsUploadRequest groups the local file path and destination object key for GCS uploads.
type gcsUploadRequest struct {
	localPath string
	gcsPath   string
}

// uploadPayloadSource identifies the target BigQuery table and source NDJSON file path.
type uploadPayloadSource struct {
	table    string
	filePath string
}

// jsonTypeLookup wraps a JSON type name for conversion to a BigQuery FieldType.
type jsonTypeLookup struct {
	typeName string
}

func (s *BigQueryServer) jsonTypeToBQType(lookup jsonTypeLookup) bigquery.FieldType {
	switch lookup.typeName {
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

// CreateEmptyTable creates the target dataset and base table with __row_id if they do not yet exist.
func (s *BigQueryServer) CreateEmptyTable(name string) error {
	ctx := context.Background()
	res := strings.Split(name, ".")
	if len(res) != 2 {
		log.Error().Str("table", name).Msg("CreateEmptyTable: table name should be in the format dataset.table")
		return fmt.Errorf("table name should be in the format dataset.table")
	}

	meta := &bigquery.DatasetMetadata{
		Location: s.Location,
	}
	if err := s.conn.Dataset(res[0]).Create(ctx, meta); err != nil && !strings.Contains(err.Error(), "Error 409: Already Exists") {
		log.Error().Err(err).Str("dataset", res[0]).Msg("CreateEmptyTable: failed to create Dataset")
		return err
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (__row_id BIGINT)", name)
	_, err := s.conn.Query(query).Read(ctx)
	if err != nil {
		log.Error().Err(err).Str("query", query).Msg("CreateEmptyTable: failed to create Table")
		return err
	}

	return nil
}

func (s *BigQueryServer) createColumns(src uploadPayloadSource, jsonTypes map[string]string) error {
	ctx := context.Background()

	for colName, jsonType := range jsonTypes {
		colType := s.jsonTypeToBQType(jsonTypeLookup{typeName: jsonType})

		query := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN IF NOT EXISTS `%s` %s", src.table, colName, colType)
		_, err := s.conn.Query(query).Read(ctx)
		if err != nil {
			log.Error().Err(err).Str("query", query).Msg("createColumns: cannot run query")
			return err
		}
		time.Sleep(2 * time.Second)
	}

	return nil
}

// CreateColumns infers column types from an NDJSON file and alters the BigQuery table schema accordingly.
func (s *BigQueryServer) CreateColumns(table string, fileName string) error {
	src := uploadPayloadSource{table: table, filePath: fileName}
	jsonTypes, err := inferFileJSONTypes(src)
	if err != nil {
		log.Error().Err(err).Str("filename", fileName).Msg("CreateColumns: Unable to infer JSON types")
		return err
	}

	if err := s.createColumns(src, jsonTypes); err != nil {
		log.Error().Err(err).Str("table", table).Msg("CreateColumns: Failed to create columns")
		return err
	}

	return nil
}

func inferFileJSONTypes(src uploadPayloadSource) (map[string]string, error) {
	input, err := os.Open(src.filePath)
	if err != nil {
		return nil, err
	}
	defer input.Close()
	return util.GetJSONTypes(input)
}

// UploadAndStream stages an NDJSON file in GCS (applying BigQuery type coercion when StrictTypes is false) and loads it.
func (s *BigQueryServer) UploadAndStream(table string, filePath string) error {
	return s.uploadAndStreamFromSource(uploadPayloadSource{table: table, filePath: filePath})
}

func (s *BigQueryServer) uploadAndStreamFromSource(src uploadPayloadSource) error {
	client, err := gcs.NewStorage(map[string]any{
		"bucket":           s.GCSBucketName,
		"credentials_json": s.CredentialsJsonString,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to create GCS client")
		return err
	}

	jsonTypes, err := inferFileJSONTypes(src)
	if err != nil {
		log.Error().Err(err).Str("filename", src.filePath).Msg("Upload And Stream: Unable to infer JSON types")
		return err
	}

	uploadFilePath, targetColTypes, cleanup, err := s.prepareUploadPayload(src, jsonTypes)
	if err != nil {
		return err
	}
	defer cleanup()

	uploadReq := gcsUploadRequest{
		localPath: uploadFilePath,
		gcsPath:   s.buildGCSObjectKey(src),
	}
	if err := s.uploadToGCS(uploadReq, client); err != nil {
		return err
	}

	loadReq := bqStreamLoadRequest{
		table:       src.table,
		gcsFilePath: uploadReq.gcsPath,
		colTypes:    targetColTypes,
	}
	if err := s.streamDataWithColumnTypes(loadReq); err != nil {
		log.Error().Err(err).Msg("Failed to stream data to BigQuery")
		return err
	}

	s.cleanupGCSObjectIfConfigured(client, uploadReq)
	return nil
}

func (s *BigQueryServer) prepareUploadPayload(src uploadPayloadSource, jsonTypes map[string]string) (string, map[string]bigquery.FieldType, func(), error) {
	noop := func() {}
	inferredBQ := s.mapInferredBQTypes(jsonTypes)
	if s.StrictTypes {
		return src.filePath, inferredBQ, noop, nil
	}
	existingCols, err := s.Columns(src.table)
	if err != nil {
		log.Warn().Err(err).Str("table", src.table).Msg("Unable to fetch existing BigQuery columns; falling back to inferred types")
	}
	resolvedTypes := resolveColumnTypes(existingCols, inferredBQ)
	coercedPath, err := coerceNDJSONFile(src.filePath, resolvedTypes)
	if err != nil {
		log.Error().Err(err).Str("file", src.filePath).Msg("Failed to coerce NDJSON values for BigQuery schema")
		return "", nil, noop, err
	}
	cleanup := func() {
		_ = os.Remove(coercedPath)
	}
	return coercedPath, resolvedTypes, cleanup, nil
}

func (s *BigQueryServer) mapInferredBQTypes(jsonTypes map[string]string) map[string]bigquery.FieldType {
	mapped := make(map[string]bigquery.FieldType, len(jsonTypes))
	for colName, jsonType := range jsonTypes {
		mapped[colName] = s.jsonTypeToBQType(jsonTypeLookup{typeName: jsonType})
	}
	return mapped
}

func (s *BigQueryServer) buildGCSObjectKey(src uploadPayloadSource) string {
	prefix := ""
	if s.GCSFilePrefix != "" {
		prefix = s.GCSFilePrefix + "/"
	}
	return prefix + src.table + "/" + filepath.Base(src.filePath)
}

func (s *BigQueryServer) uploadToGCS(req gcsUploadRequest, client *gcs.Storage) error {
	log.Info().Msg("Uploading file to GCS ")
	file, err := os.Open(req.localPath)
	if err != nil {
		log.Error().Err(err).Str("file", req.localPath).Msg("Failed to open file")
		return err
	}
	defer file.Close()

	if err := client.Upload(req.gcsPath, file); err != nil {
		log.Error().Err(err).Str("file", req.localPath).Str("gcs_file", req.gcsPath).Msg("Failed to upload file to GCS")
		return err
	}
	log.Info().Str("gcs_file", req.gcsPath).Msg("Uploaded file to GCS")
	return nil
}

func (s *BigQueryServer) cleanupGCSObjectIfConfigured(client *gcs.Storage, req gcsUploadRequest) {
	if !s.DeleteFromGCS {
		return
	}
	log.Info().Msg("Deleting file from GCS")
	if err := client.Delete(req.gcsPath); err != nil {
		log.Error().Err(err).Str("gcs_file", req.gcsPath).Msg("Failed to delete file from GCS")
		return
	}
	log.Info().Msg("Deleted file from GCS")
}

func (s *BigQueryServer) streamDataWithColumnTypes(req bqStreamLoadRequest) error {
	location := fmt.Sprintf("gs://%s/%s", s.GCSBucketName, req.gcsFilePath)
	columns := formatBQColumnsClause(req.colTypes)
	query := fmt.Sprintf("LOAD DATA INTO %s %s FROM FILES ( format = 'JSON', uris = ['%s'] )", req.table, columns, location)
	if _, err := s.conn.Query(query).Read(context.Background()); err != nil {
		log.Error().Err(err).Str("query", query).Msg("StreamDataToBigQuery: failed to stream data to BigQuery")
		return err
	}
	log.Info().Msg("Successfully loaded data into BigQuery")
	return nil
}

func formatBQColumnsClause(colTypes map[string]bigquery.FieldType) string {
	parts := make([]string, 0, len(colTypes))
	for colName, colType := range colTypes {
		parts = append(parts, fmt.Sprintf("`%s` %s", colName, colType))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// InsertFromNDJsonFile uploads an NDJSON file to GCS and streams it into the target BigQuery table.
func (s *BigQueryServer) InsertFromNDJsonFile(table string, filePath string) error {
	src := uploadPayloadSource{table: table, filePath: filePath}
	if err := s.uploadAndStreamFromSource(src); err != nil {
		log.Error().Err(err).Str("table", table).Str("file", filePath).Msg("Failed to upload and stream data to BigQuery")
		return err
	}
	return nil
}
