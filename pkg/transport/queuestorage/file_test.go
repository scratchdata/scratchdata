package queuestorage_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"scratchdata/pkg/transport/queuestorage"
	"scratchdata/pkg/transport/queuestorage/testfiles/mocks"
)

var testTime = time.Now().UTC()

func testTimeProvider() time.Time {
	return testTime
}

func TestFileWriter(t *testing.T) {
	param := queuestorage.NewFileWriterParam{
		Key:         "testKey",
		Path:        filepath.Join(t.TempDir(), "testdata.ndjson"),
		Store:       &mocks.TestStorage{},
		Queue:       &mocks.TestQueue{},
		Notify:      nil,
		MaxFileSize: 0,
		MaxRows:     0,
		Expiry:      testTimeProvider().Add(5 * time.Second),
	}

	var writer *queuestorage.FileWriter
	t.Run("require non-optional parameter", func(t *testing.T) {
		var err error
		writer, err = queuestorage.NewFileWriter(param)
		assert.Error(t, err, "should fail because notify channel is not set")
	})

	param.Notify = make(chan queuestorage.FileWriterInfo)
	writer, err := queuestorage.NewFileWriter(param)
	require.NoError(t, err)

	t.Run("defaults are set", func(t *testing.T) {
		info := writer.Info()
		require.Equal(t, param.Key, info.Key)
		require.Equal(t, param.Path, info.Path)
		require.Equal(t, param.Expiry, info.Expiry)
		require.Equal(t, queuestorage.MaxFileSize, info.MaxFileSize, "should use default MaxFileSize when not set")
		require.Equal(t, queuestorage.MaxRows, info.MaxRows, "should use default MaxRows when not set")
	})

	t.Run("file was created", func(t *testing.T) {
		info := writer.Info()
		assert.FileExists(t, info.Path)
	})

	t.Run("file is writable", func(t *testing.T) {
		data := []byte(`{"data": "testing"}`)
		n, err := writer.WriteLn(data)

		assert.NoError(t, err, "write to file should not fail")
		assert.Equal(t, len(data)+1, n, "WriteLn should write all bytes plus newline")

		info := writer.Info()
		bb, err := os.ReadFile(info.Path)
		assert.NoError(t, err, "should be able to read file")
		assert.Equal(t, append(data, '\n'), bb)
	})

	t.Run("file was closed", func(t *testing.T) {
		go func() {
			if err := writer.Close(); err != nil {
				log.Err(err).Msg("failed to close writer")
			}
		}()

		closedFile := <-param.Notify // blocks until writer is closed
		assert.FileExists(t, closedFile.Path)
	})
}
