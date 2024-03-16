package bigquery_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/scratchdata/scratchdata/pkg/destinations/bigquery"
)

func TestBigQueryServer(t *testing.T) {
	testID := strings.ToLower(ulid.Make().String())
	tableName := "bqMockTable"
	settings := map[string]any{
		"project_id":      os.Getenv("GCP_PROJECT_ID"),
		"dataset_id":      "scratchdata_bq_test_" + testID,
		"credential_file": os.Getenv("GCP_SERVICE_CREDENTIAL_FILE"),
		"location":        "",
		"max_open_conns":  1,
	}
	server, err := bigquery.OpenServer(settings)
	require.NoError(t, err, "bigquery server connection failed")

	insertData := `
	{"name": "John", "text": "sample data", "integer": 123}
	{"name": "Doe", "text": "sample data", "integer": 321}
`
	rs := strings.NewReader(insertData)
	err = server.InsertBatchFromNDJson(tableName, rs)
	require.NoError(t, err, "bigquery insertion failed")

	fqtn := fmt.Sprintf("`%s.%s`", server.DatasetID, tableName)
	querySQL := fmt.Sprintf(`
		SELECT
			*
		FROM %s
		LIMIT 10;
	`, fqtn)

	var out bytes.Buffer
	err = server.QueryJSON(querySQL, &out)
	require.NoError(t, err, "bigquery query failed")

	assert.True(t, json.Valid(out.Bytes()))
	expected := "[" +
		`{"name": "John", "text": "sample data", "integer": 123}` +
		`{"name": "Doe", "text": "sample data", "integer": 321}` +
		"]"

	assert.Equal(t, expected, out.String(), "validated bigquery table data")
}

