package queuestorage

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"scratchdata/models"
	memDest "scratchdata/pkg/destinations/memory"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"
)

type testDB struct{}

func (d testDB) Open() error { return nil }

func (d testDB) Close() error { return nil }

func (d testDB) Hash(input string) string { return input }

func (d testDB) GetAPIKeyDetails(hashedKey string) models.APIKey { return models.APIKey{} }

func (d testDB) GetAccount(id string) models.Account { return models.Account{} }

func (d testDB) GetDatabaseConnections(accountID string) []models.DatabaseConnection { return nil }

func (d testDB) GetDatabaseConnection(connectionID string) models.DatabaseConnection {
	return models.DatabaseConnection{ID: "test", Type: "memory"}
}

func TestQueueStorageTransportConsumer(t *testing.T) {
	queue := memQ.NewQueue()
	store := memFS.NewStorage()
	dbsrv := memDest.MemoryDBServer{}
	param := QueueStorageParam{
		Queue:                  queue,
		Storage:                store,
		DB:                     testDB{},
		ConsumerDataDir:        t.TempDir(),
		DequeueTimeout:         10 * time.Millisecond,
		FreeSpaceRequiredBytes: 100 << 20,
		Workers:                1,
	}

	qs := NewQueueStorageTransport(param)
	path := "/msg"
	uploadData := []byte(`{"data":"hello world"}`)
	table := "tbl"
	queueMsg, err := json.Marshal(models.FileUploadMessage{
		Path:  path,
		Key:   "test",
		Table: table,
	})
	expectRes := []byte(`[{"data":"hello world"}]`)
	if err != nil {
		t.Fatalf("Cannot marshal queueMsg: %s", err)
	}

	if err := qs.StartConsumer(); err != nil {
		t.Fatalf("Cannot start consumer: %s", err)
	}

	if err := store.Upload(path, bytes.NewReader(uploadData)); err != nil {
		t.Errorf("Cannot upload data: %s", err)
	}
	if err := queue.Enqueue(queueMsg); err != nil {
		t.Errorf("Cannot enqueue message: %s", err)
	}

	queryRes := bytes.NewBuffer(nil)
	for i := 0; i < 20 && queryRes.Len() == 0; i++ {
		// wait for data to be processed. it's all in-memory so shouldn't take long
		time.Sleep(50 * time.Millisecond)

		err := dbsrv.QueryJSON(`select * from tbl`, queryRes)
		if err != nil {
			t.Errorf("Cannot query db: %s", err)
			break
		}
	}

	if !bytes.Equal(queryRes.Bytes(), expectRes) {
		t.Errorf("Expected query result %#q; Got %#q", expectRes, queryRes.Bytes())
	}

	if err := qs.StopConsumer(); err != nil {
		t.Errorf("Cannot stop consumer: %s", err)
	}
}
