package queuestorage

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"
)

var testTime = time.Now().UTC()

func testTimeProvider() time.Time {
	return testTime
}

func TestFileWriter(t *testing.T) {
	param := FileWriterParam{
		Key:         "testKey",
		Dir:         t.TempDir(),
		MaxFileSize: 144,
		queue:       memQ.NewQueue(),
		storage:     memFS.NewStorage(),
	}

	writer, err := NewFileWriter(param)
	require.NoError(t, err)

	t.Run("defaults are set", func(t *testing.T) {
		info := writer.Info()
		assert.Equal(t, param.Key, info.Key)
		assert.False(t, info.Closed)
	})

	t.Run("file was created", func(t *testing.T) {
		info := writer.Info()
		assert.FileExists(t, info.Path)
	})

	t.Run("reject oversize data", func(t *testing.T) {
		data := []byte(`{"data":"testing","data2":"testing2"}`)
		total, err := writer.Write(data)
		assert.Error(t, err, "write should fail when it exceeds the max size")
		assert.Equal(t, 0, total)
	})

	t.Run("file is writable", func(t *testing.T) {
		data := []byte(`{"data":"testing"}`)

		total, err := writer.Write(data)
		require.NoError(t, err, "write to file should not fail")

		n := len(data)
		info := writer.Info()
		actual := n + len(`,"__row_id":"","__batch_file":""`) +
			len(info.Path) +
			26 + // ulid length
			1 // newline
		assert.Equal(t, actual, total, "Write should write all bytes and ids plus newline")

		bb, err := os.ReadFile(info.Path)
		require.NoError(t, err, "should be able to read file")
		assert.Equal(t, data[:n-1], bb[:n-1])

		pattern := fmt.Sprintf(`"__row_id":"\w{26}","__batch_file":"%s+"`, info.Path)
		assert.Regexp(t, regexp.MustCompile(pattern), string(bb[n-1:]))
	})

	info := writer.Info()
	t.Run("file was closed", func(t *testing.T) {
		err := writer.Close()
		require.NoError(t, err, "close should not fail")
	})

	t.Run("file was pushed to queue", func(t *testing.T) {
		bb, err := param.queue.Dequeue()
		require.NoError(t, err)

		msg := fmt.Sprintf(`{"key":"%s","path":"%s"}`, info.Key, info.Path)
		assert.Equal(t, []byte(msg), bb)

	})

	t.Run("file was uploaded to storage", func(t *testing.T) {
		download, err := param.storage.Download(info.Path)
		require.NoError(t, err)

		bb, err := os.ReadFile(info.Path)
		require.NoError(t, err, "should be able to read file")

		expected, err := io.ReadAll(download)
		require.NoError(t, err)
		assert.Equal(t, bb, expected)
	})
}
