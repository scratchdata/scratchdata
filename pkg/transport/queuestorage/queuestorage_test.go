package queuestorage_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdata/models"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"

	. "scratchdata/pkg/transport/queuestorage"
)

func TestQueueStorageTransportProducer(t *testing.T) {
	param := QueueStorageParam{
		Queue:   memQ.NewQueue(),
		Storage: memFS.NewStorage(),
		WriterOpt: WriterOptions{
			DataDir: t.TempDir(),
		},
	}
	qs := NewQueueStorageTransport(param)

	err := qs.StartProducer()
	require.NoError(t, err)

	var (
		ids     = []string{"testA", "testB", "testC"}
		rowSize = 5
		tmpl    = `{"id":"%s","index":%d}`
	)

	for _, id := range ids {
		go func(id string) {
			for i := 0; i < rowSize; i++ {
				msg := []byte(fmt.Sprintf(tmpl, id, i))
				err := qs.Write(id, "", msg)
				require.NoError(t, err)
			}
		}(id)
	}

	// Wait for writes to complete before closing
	time.Sleep(1 * time.Second)
	err = qs.StopProducer()
	require.NoError(t, err)

	for _, _ = range ids {
		bb, err := param.Queue.Dequeue()
		require.NoError(t, err)

		var msg models.FileUploadMessage
		require.NoError(t, json.Unmarshal(bb, &msg))

		bb, err = os.ReadFile(msg.Path)
		require.NoError(t, err)

		parts := bytes.SplitN(bb, []byte{'\n'}, rowSize)
		require.Equal(t, rowSize, len(parts))

		for _, part := range parts {
			t.Run("content matches", func(t *testing.T) {
				pattern := fmt.Sprintf(`\{"id":"%s","index":\d,`, msg.Key)
				assert.Regexp(t, regexp.MustCompile(pattern), string(part))
			})
		}
	}
}
