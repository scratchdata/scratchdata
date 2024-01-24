package bigquery_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdata/pkg/destinations/bigquery"
)

func TestBigQueryServer(t *testing.T) {
	testID := strings.ToLower(ulid.Make().String())
	tableName := "bq_testdata"
	settings := map[string]any{
		"project_id":      os.Getenv("GCP_PROJECT_ID"),
		"dataset_id":      "scratchdata_test_" + testID,
		"credential_file": os.Getenv("GCP_SERVICE_CREDENTIAL_FILE"),
		"location":        "",
		"max_open_conns":  1,
	}
	server, err := bigquery.OpenServer(settings)
	require.NoError(t, err, "bigquery server connection failed")

	insertData := `
	{"name": "Ed", "text": "Knock knock.", "boolTrue": true, "boolFalse": false, "floating": 1.99999999999999, "integer": 999}
	{"name": "Sam", "text": "Who's there?", "boolTrue": true, "boolFalse": false, "floating": 1.99999999999999, "integer": 999}
	{"name": "Ed", "text": "Go fmt.", "boolTrue": true, "boolFalse": false, "floating": 1.99999999999999, "integer": 999}
	{"name": "Sam", "text": "Go fmt who?", "boolTrue": true, "boolFalse": false, "floating": 1.99999999999999, "integer": 999}
	{"name": "Ed", "text": "Go fmt yourself!", "boolTrue": true, "boolFalse": false, "floating": 1.99999999999999, "integer": 999}
`
	rs := strings.NewReader(insertData)
	err = server.InsertBatchFromNDJson(tableName, rs)
	require.NoError(t, err, "bigquery insertion failed")

	fqtn := fmt.Sprintf("`%s.%s`", server.DatasetID, tableName)
	querySQL := fmt.Sprintf(`
		SELECT
			Name,
			Text,
			boolTrue,
			boolFalse,
			floating,
			integer,
		FROM %s
		LIMIT 10;
	`, fqtn)

	var out bytes.Buffer
	err = server.QueryJSON(querySQL, &out)
	require.NoError(t, err, "bigquery query failed")

	assert.True(t, json.Valid(out.Bytes()))
	expected := "[" +
		`{"name":"Ed","text":"Knock knock.","boolTrue":true,"boolFalse":false,"floating":1.99999999999999,"integer":999},` +
		`{"name":"Sam","text":"Who's there?","boolTrue":true,"boolFalse":false,"floating":1.99999999999999,"integer":999},` +
		`{"name":"Ed","text":"Go fmt.","boolTrue":true,"boolFalse":false,"floating":1.99999999999999,"integer":999},` +
		`{"name":"Sam","text":"Go fmt who?","boolTrue":true,"boolFalse":false,"floating":1.99999999999999,"integer":999},` +
		`{"name":"Ed","text":"Go fmt yourself!","boolTrue":true,"boolFalse":false,"floating":1.99999999999999,"integer":999}` +
		"]"

	assert.Equal(t, expected, out.String(), "bigquery data matches inserted data")
}
