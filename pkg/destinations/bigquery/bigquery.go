package bigquery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"scratchdata/util"
)

type BigQueryServer struct {
	// ProjectID is Google Cloud project identifier
	ProjectID string `mapstructure:"project_id"`

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

func (b *BigQueryServer) createTable(ctx context.Context, table string) error {
	dataset := b.client.Dataset(table)
	metadata := bigquery.DatasetMetadata{
		Location: b.Location,
	}
	if err := dataset.Create(ctx, &metadata); err != nil {
		return err
	}
	return nil
}

func (b *BigQueryServer) getColumnNames(ctx context.Context, table string) (map[string]bool, error) {
	meta, err := b.client.Dataset(table).Table(table).Metadata(ctx)
	if err != nil {
		return nil, err
	}

	columns := make(map[string]bool)
	for _, field := range meta.Schema {
		columns[field.Name] = true
	}

	return columns, nil
}

func (b *BigQueryServer) createColumns(ctx context.Context, table string, jsonTypes map[string]string) error {
	existingColumns, err := b.getColumnNames(ctx, table)
	if err != nil {
		return err
	}

	tableRef := b.client.Dataset(table).Table(table)
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}

	jsonT := map[string]bigquery.FieldType{
		"string": bigquery.StringFieldType,
		"int":    bigquery.IntegerFieldType,
		"float":  bigquery.FloatFieldType,
		"bool":   bigquery.BooleanFieldType,
	}

	var metaUpdate bigquery.TableMetadataToUpdate
	for columnName, columnType := range jsonTypes {
		// ignore existing columns
		if existingColumns[strings.ToLower(columnName)] {
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
		metaUpdate.Schema = append(meta.Schema, &field)
	}

	if _, err := tableRef.Update(ctx, metaUpdate, meta.ETag); err != nil {
		return err
	}

	return nil
}

func (b *BigQueryServer) InsertBatchFromNDJson(table string, input io.ReadSeeker) error {
	ctx := context.TODO()
	types, err := util.GetJSONTypes(input)
	if err != nil {
		return err
	}
	_, _ = input.Seek(0, 0)

	if err := b.createTable(ctx, table); err != nil {
		return err
	}

	if err := b.createColumns(ctx, table, types); err != nil {
		return err
	}

	// TODO: read input and insert
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
	for {
		it, err := q.Read(ctx)
		if err != nil {
			return fmt.Errorf("query.Read(): %w", err)
		}

		var row map[string]bigquery.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return err
		}
		if !firstRow {
			_, _ = writer.Write([]byte(","))
			firstRow = false
		}
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("bigquery.QueryJSON: Cannot encode row: %w", err)
		}
		_, _ = writer.Write(buffer.Bytes())
		buffer.Reset()
	}
	_, _ = writer.Write([]byte("]"))
	return nil
}

func (b *BigQueryServer) Close() error {
	return b.client.Close()
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

func OpenServer(settings map[string]any) (*BigQueryServer, error) {
	srv := util.ConfigToStruct[BigQueryServer](settings)
	if err := srv.connect(); err != nil {
		return nil, fmt.Errorf("bigquery.OpenServer: %w", err)
	}
	return srv, nil
}
