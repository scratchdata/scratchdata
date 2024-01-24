package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"github.com/oklog/ulid/v2"
	"golang.org/x/exp/maps"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"scratchdata/util"
)

type BigQueryServer struct {
	// ProjectID is Google Cloud project identifier
	ProjectID string `mapstructure:"project_id"`

	// DatasetID is the target dataset identifier
	DatasetID string `mapstructure:"dataset_id"`

	// CredentialFile authenticates API calls with the given service account
	// or refresh token JSON credentials file.
	CredentialFile string `mapstructure:"credential_file"`

	// Location
	Location string `mapstructure:"location"`

	// MaxOpenConns sets the maximum number for a pool of gRPC connections
	// that requests will be balanced between.
	MaxOpenConns int `mapstructure:"max_open_conns"`

	client *bigquery.Client
}

func (b *BigQueryServer) createDataset(ctx context.Context) (err error) {
	dataset := b.client.Dataset(b.DatasetID)

	// Fetch dataset metadata or create if it doesn't exist
	var metadata *bigquery.DatasetMetadata
	if metadata, err = dataset.Metadata(ctx); err != nil {
		metadata = &bigquery.DatasetMetadata{
			Location: b.Location,
		}
		if errCreateDataset := dataset.Create(ctx, metadata); err != nil {
			return errors.Join(err, errCreateDataset)
		}
	}
	return nil
}

func (b *BigQueryServer) createTable(ctx context.Context, name string) error {
	if err := b.createDataset(ctx); err != nil {
		return fmt.Errorf("unable to create dataset: %w", err)
	}

	table := b.client.Dataset(b.DatasetID).Table(name)

	// Fetch table metadata
	if metadata, err := table.Metadata(ctx); err != nil {
		// TODO: Properly check error codes
		//var apiError *googleapi.Error
		//if errors.As(err, &apiError) && apiError.Code == http.StatusNotFound {
		//
		//}
		if errCreateTable := table.Create(ctx, metadata); err != nil {
			return errors.Join(err, errCreateTable)
		}
	}
	return nil
}

func (b *BigQueryServer) getColumnNames(ctx context.Context, tableName string) (map[string]bool, error) {
	tableMetadata, err := b.client.Dataset(b.DatasetID).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, err
	}

	columns := make(map[string]bool)
	for _, field := range tableMetadata.Schema {
		columns[field.Name] = true
	}

	return columns, nil
}

func (b *BigQueryServer) createColumns(ctx context.Context, tableName string, jsonTypes map[string]string) (bigquery.Schema, error) {
	existingColumns, err := b.getColumnNames(ctx, tableName)
	if err != nil {
		return bigquery.Schema{}, err
	}

	tableRef := b.client.Dataset(b.DatasetID).Table(tableName)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return bigquery.Schema{}, err
	}

	jsonT := map[string]bigquery.FieldType{
		"string": bigquery.StringFieldType,
		"int":    bigquery.IntegerFieldType,
		"float":  bigquery.FloatFieldType,
		"bool":   bigquery.BooleanFieldType,
	}

	var metaUpdate = bigquery.TableMetadataToUpdate{
		Schema: meta.Schema,
	}
	for columnName, columnType := range jsonTypes {
		// ignore existing columns
		if existingColumns[columnName] {
			continue
		}

		// ignore unknown type
		fieldType, ok := jsonT[columnType]
		if !ok {
			continue
		}

		field := bigquery.FieldSchema{
			Name: columnName,
			Type: fieldType,
		}
		metaUpdate.Schema = append(metaUpdate.Schema, &field)
	}

	if _, err := tableRef.Update(ctx, metaUpdate, meta.ETag); err != nil {
		return bigquery.Schema{}, err
	}

	return metaUpdate.Schema, nil
}

func (b *BigQueryServer) connect() error {
	client, err := bigquery.NewClient(
		context.TODO(), b.ProjectID,
		option.WithCredentialsFile(b.CredentialFile),
		option.WithGRPCConnectionPool(b.MaxOpenConns),
	)
	if err != nil {
		return fmt.Errorf("unable to establish a client for %s: %w", b.ProjectID, err)
	}
	b.client = client
	return nil
}

func (b *BigQueryServer) InsertBatchFromNDJson(tableName string, input io.ReadSeeker) error {
	ctx := context.TODO()
	types, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}
	_, _ = input.Seek(0, 0)

	if err := b.createTable(ctx, tableName); err != nil {
		return err
	}

	schema, err := b.createColumns(ctx, tableName, types)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(input)
	var rows []bigquery.ValueSaver
	for decoder.More() {
		var data map[string]bigquery.Value
		if err := decoder.Decode(&data); err != nil {
			return err
		}

		rows = append(rows, &bigquery.ValuesSaver{
			Row:      maps.Values(data),
			Schema:   schema,
			InsertID: ulid.Make().String(),
		})
	}

	// Write data to BigQuery
	inserter := b.client.Dataset(b.DatasetID).Table(tableName).Inserter()
	if err := inserter.Put(ctx, rows); err != nil {
		return err
	}
	b.client.Dataset(b.DatasetID).Table(tableName).Uploader()

	return nil
}

func (b *BigQueryServer) QueryJSON(query string, writer io.Writer) error {
	ctx := context.TODO()

	sql := fmt.Sprintf("SELECT * FROM (%s)", util.TrimQuery(query))
	q := b.client.Query(sql)
	q.Location = b.Location

	_, _ = writer.Write([]byte("["))
	firstRow := true
	buffer := bytes.NewBuffer(nil)
	encoder := json.NewEncoder(buffer)
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("query.Read(): %w", err)
	}

	for {
		var row map[string]bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return err
		}
		if !firstRow {
			_, _ = writer.Write([]byte(","))
		}
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("bigquery.QueryJSON: Cannot encode row: %w", err)
		}
		_, _ = writer.Write(buffer.Bytes())
		firstRow = false
		buffer.Reset()
	}
	_, _ = writer.Write([]byte("]"))
	return nil
}

func (b *BigQueryServer) Close() error {
	return b.client.Close()
}

func OpenServer(settings map[string]any) (*BigQueryServer, error) {
	srv := util.ConfigToStruct[BigQueryServer](settings)
	if err := srv.connect(); err != nil {
		return nil, fmt.Errorf("bigquery.OpenServer: %w", err)
	}
	return srv, nil
}
