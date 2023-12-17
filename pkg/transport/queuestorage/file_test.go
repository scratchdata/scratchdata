package queuestorage_test

import (
	"bytes"
	"encoding/json"
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

	"scratchdata/pkg/transport/queuestorage"
)

var testTime = time.Now().UTC()

func testTimeProvider() time.Time {
	return testTime
}

func TestFileWriter(t *testing.T) {
	param := queuestorage.FileWriterParam{
		Key:         "testKey",
		Dir:         t.TempDir(),
		MaxFileSize: 144,
		Queue:       memQ.NewQueue(),
		Storage:     memFS.NewStorage(),
	}

	writer, err := queuestorage.NewFileWriter(param)
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
	t.Run("file was terminated", func(t *testing.T) {
		err := writer.Close()
		require.NoError(t, err, "close should not fail")
	})

	t.Run("no writes after termination", func(t *testing.T) {
		_, err := writer.Write([]byte{})
		assert.Error(t, err)
	})

	t.Run("no rotation after termination", func(t *testing.T) {
		assert.Equal(t, writer.Info().Path, info.Path)
	})

	t.Run("file was pushed to queue", func(t *testing.T) {
		bb, err := param.Queue.Dequeue()
		require.NoError(t, err)

		msg := fmt.Sprintf(`{"key":"%s","path":"%s"}`, info.Key, info.Path)
		assert.Equal(t, []byte(msg), bb)
	})

	t.Run("file was uploaded to storage", func(t *testing.T) {
		download, err := param.Storage.Download(info.Path)
		require.NoError(t, err)

		bb, err := os.ReadFile(info.Path)
		require.NoError(t, err, "should be able to read file")

		expected, err := io.ReadAll(download)
		require.NoError(t, err)
		assert.Equal(t, bb, expected)
	})
}

func TestFileWriterAutoRotation(t *testing.T) {
	param := queuestorage.FileWriterParam{
		Key:     "testKey",
		Dir:     t.TempDir(),
		Queue:   memQ.NewQueue(),
		Storage: memFS.NewStorage(),
	}

	checkRotation := func(t *testing.T, param queuestorage.FileWriterParam, inter func()) {
		w, err := queuestorage.NewFileWriter(param)
		require.NoError(t, err)

		tmpl := `{"data":"test-%d"}`
		info := [2]queuestorage.FileWriterInfo{}
		for i := 0; i < 2; i++ {
			msg := fmt.Sprintf(tmpl, i)
			_, err := w.Write([]byte(msg))
			info[i] = w.Info()
			require.NoError(t, err)
			if inter != nil {
				inter()
			}
		}

		t.Run("different file path", func(t *testing.T) {
			assert.Equal(t, param.Key, info[0].Key)
			assert.Equal(t, info[0].Key, info[1].Key)
			assert.NotEqual(t, info[0].Path, info[1].Path)
		})

		require.NoError(t, w.Close())

		for i := 0; i < 2; i++ {
			var err error
			d, err := param.Storage.Download(info[i].Path)
			require.NoError(t, err)

			dd, err := io.ReadAll(d)
			require.NoError(t, err)

			expected := fmt.Sprintf(tmpl, i)

			t.Run("content in files matches", func(t *testing.T) {
				assert.Contains(t, string(dd), expected[:len(expected)-1])
			})
		}
	}

	t.Run("rotation by age", func(t *testing.T) {
		param := param
		param.MaxFileAge = 1 * time.Second
		checkRotation(t, param, func() {
			// +10ms to ensure rotation completes from timer expiry
			time.Sleep(param.MaxFileAge + (10 * time.Millisecond))
		})
	})

	t.Run("rotation by size", func(t *testing.T) {
		param := param
		param.MaxFileSize = 144 * 2
		checkRotation(t, param, nil)
	})

	t.Run("rotation by row", func(t *testing.T) {
		param := param
		param.MaxRows = 1
		checkRotation(t, param, nil)
	})
}

func TestFileWriterMultipleWrite(t *testing.T) {
	param := queuestorage.FileWriterParam{
		Key:     "testKey",
		Dir:     t.TempDir(),
		Queue:   memQ.NewQueue(),
		Storage: memFS.NewStorage(),
	}
	w, err := queuestorage.NewFileWriter(param)
	require.NoError(t, err)

	tmpl := `{"data":"test-%d"}`
	info := w.Info()
	for i := 0; i < 2; i++ {
		msg := fmt.Sprintf(tmpl, i)
		_, err := w.Write([]byte(msg))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())

	bb, err := os.ReadFile(info.Path)
	require.NoError(t, err)

	parts := bytes.SplitN(bb, []byte{'\n'}, 2)
	require.Equal(t, 2, len(parts))

	lines := [2]map[string]string{}
	for i, part := range parts {
		lines[i] = map[string]string{}
		err := json.Unmarshal(part, &lines[i])
		require.NoError(t, err)
	}
	l0, l1 := lines[0], lines[1]

	t.Run("data matches", func(t *testing.T) {
		assert.Equal(t, "test-0", l0["data"])
		assert.Equal(t, "test-1", l1["data"])
	})

	t.Run("batch_file are same", func(t *testing.T) {
		assert.Equal(t, l0["__batch_file"], l1["__batch_file"])
	})

	t.Run("written row ids are different", func(t *testing.T) {
		assert.NotEqual(t, l0["__row_id"], l1["__row_id"])
	})
}