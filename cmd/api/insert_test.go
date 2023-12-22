package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdata/config"
	"scratchdata/models"
	"scratchdata/pkg/database"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"
	"scratchdata/pkg/transport/queuestorage"
)

type testDB struct {
	database.Database
}

func (d testDB) Hash(input string) string { return input }

func (d testDB) GetAPIKeyDetails(hashedKey string) models.APIKey {
	return models.APIKey{
		ID:           hashedKey,
		HashedAPIKey: hashedKey,
	}
}

func (d testDB) GetDatabaseConnection(connectionID string) models.DatabaseConnection {
	return models.DatabaseConnection{ID: "test", Type: "memory"}
}

func TestAPI_Insert(t *testing.T) {
	cfg := config.API{
		Enabled:                true,
		Port:                   0,
		DataDir:                "",
		MaxAgeSeconds:          0,
		MaxSizeBytes:           0,
		HealthCheckPath:        "",
		FreeSpaceRequiredBytes: 0,
	}
	db := testDB{}
	qs := queuestorage.NewQueueStorageTransport(queuestorage.QueueStorageParam{
		Queue:   memQ.NewQueue(),
		Storage: memFS.NewStorage(),
		WriterOpt: queuestorage.WriterOptions{
			DataDir:     t.TempDir(),
			MaxFileSize: 20,
			MaxRows:     100,
			MaxFileAge:  time.Hour,
		},
	})
	api := NewAPIServer(cfg, db, qs)

	const apiKey = "testAPIKey"
	testCases := []struct {
		name    string
		payload string
		header  http.Header
		query   url.Values

		statusCode int
		respBody   string
	}{
		{
			name:       "Unauthorised request",
			payload:    `{"property":"A"}`,
			statusCode: 401,
			respBody:   "Unauthorized",
		},
		{
			name:    "Read table and flatten from header",
			payload: `[{"property":"A"},{"property":"B"}]`,
			header: http.Header{
				API_KEY_HEADER:  []string{apiKey},
				TableNameHeader: []string{"testTable"},
				FlattenHeader:   []string{"explode"},
			},
			query:      url.Values{},
			statusCode: 200,
			respBody:   "ok",
		},
		{
			name:    "Read table and flatten from url",
			payload: `[{"property":"A"},{"property":"B"}]`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			query: url.Values{
				TableNameQuery: []string{"testTable"},
				FlattenQuery:   []string{"explode"},
			},
			statusCode: 200,
			respBody:   "ok",
		},
		{
			name:    "Read table and flatten from body",
			payload: `{"table":"testTable","data":{"property":"A"}}`,
			header: http.Header{
				API_KEY_HEADER: []string{apiKey},
			},
			statusCode: 200,
			respBody:   "ok",
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			payload := strings.NewReader(test.payload)
			target := "/data"
			if test.query != nil {
				target += "?" + test.query.Encode()
			}
			req := httptest.NewRequest(http.MethodPost, target, payload)
			if test.header != nil {
				req.Header = test.header
			}
			resp, err := api.app.Test(req)
			require.NoError(t, err)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, test.statusCode, resp.StatusCode)
			assert.Equal(t, test.respBody, string(body))
		})
	}
}
