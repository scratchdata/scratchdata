package queuestorage_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	memFS "scratchdata/pkg/filestore/memory"
	memQ "scratchdata/pkg/queue/memory"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"scratchdata/pkg/transport/queuestorage"
)

func TestFileWriter(t *testing.T) {
	t.Parallel()
	param := queuestorage.FileWriterParam{
		Key:         "testKey",
		Dir:         t.TempDir(),
		MaxFileSize: 500,
		MaxFileAge:  TestWriterOptions.MaxFileAge,
		MaxRows:     TestWriterOptions.MaxRows,
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

	t.Run("unique key in filepath", func(t *testing.T) {
		dir := filepath.Dir(writer.Info().Path)
		assert.True(t, strings.HasSuffix(dir, param.Key))
	})

	t.Run("file was created", func(t *testing.T) {
		info := writer.Info()
		assert.FileExists(t, info.Path)
	})

	t.Run("reject oversize data", func(t *testing.T) {
		dataMap := map[string]string{}
		for i := 0; i < 1000; i++ {
			dataMap[fmt.Sprintf("key_number_%d", i)] = "hello world how are you"
		}
		data, err := json.Marshal(dataMap)
		require.NoError(t, err, "Should be able to convert map to json")
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
		actual := n + len(`,"__batch_file":""`) +
			len(info.Path) +
			1 // newline
		assert.Equal(t, actual, total, "Write should write all bytes and ids plus newline")

		bb, err := os.ReadFile(info.Path)
		require.NoError(t, err, "should be able to read file")
		assert.Equal(t, data[:n-1], bb[:n-1])

		pattern := fmt.Sprintf(`"__batch_file":"%s+"`, info.Path)
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

		msg := fmt.Sprintf(`{"key":"%s","path":"%s","table":""}`, info.Key, info.Path)
		assert.Equal(t, msg, string(bb))
	})

	t.Run("file was uploaded to storage", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "test-*.ndjson")
		require.NoError(t, err)

		err = param.Storage.Download(info.Path, f)
		require.NoError(t, err)

		bb, err := os.ReadFile(info.Path)
		require.NoError(t, err, "should be able to read file")

		expected, err := io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		assert.Equal(t, bb, expected)
	})
}

func TestFileWriterAutoRotation(t *testing.T) {
	t.Parallel()
	param := queuestorage.FileWriterParam{
		Key:         "testKey",
		Dir:         t.TempDir(),
		MaxFileSize: TestWriterOptions.MaxFileSize,
		MaxFileAge:  TestWriterOptions.MaxFileAge,
		MaxRows:     TestWriterOptions.MaxRows,
		Queue:       memQ.NewQueue(),
		Storage:     memFS.NewStorage(),
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
			f, err := os.CreateTemp(t.TempDir(), "test-*.ndjson")
			require.NoError(t, err)

			err = param.Storage.Download(info[i].Path, f)
			require.NoError(t, err)

			dd, err := io.ReadAll(f)
			require.NoError(t, err)
			require.NoError(t, f.Close())

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
			// +1s to ensure rotation completes from timer expiry
			time.Sleep(param.MaxFileAge + (1 * time.Second))
		})
	})

	t.Run("rotation by size", func(t *testing.T) {
		// size of data written by test "file is writable"
		dataSize := int64(111)

		param := param
		param.MaxFileSize = dataSize * 2
		checkRotation(t, param, nil)
	})

	t.Run("rotation by row", func(t *testing.T) {
		param := param
		param.MaxRows = 1
		checkRotation(t, param, nil)
	})
}

func TestFileWriterMultipleWrite(t *testing.T) {
	t.Parallel()
	param := queuestorage.FileWriterParam{
		Key:         "testKey",
		Dir:         t.TempDir(),
		MaxFileSize: TestWriterOptions.MaxFileSize,
		MaxFileAge:  TestWriterOptions.MaxFileAge,
		MaxRows:     TestWriterOptions.MaxRows,
		Queue:       memQ.NewQueue(),
		Storage:     memFS.NewStorage(),
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
}
