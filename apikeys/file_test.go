package apikeys_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"scratchdb/apikeys"
)

func TestAPIKeysFromFile_GetDetailsByKey(t *testing.T) {
	keyfile := filepath.Join("testdata", "apikeys.json")
	keys := apikeys.APIKeysFromFile{FileName: keyfile}

	t.Run("get existing key", func(t *testing.T) {
		keyName := "testUser1Key"
		key, ok := keys.GetDetailsByKey(keyName)
		assert.True(t, ok)

		expected := &apikeys.APIKeyDetailsFromFile{
			Name:       "Test User1",
			APIKey:     "testUser1Key",
			DBCluster:  "testUser1Cluster",
			DBName:     "testUser1Db",
			DBUser:     "testUser1",
			DBPassword: "testUser1Password",
		}
		assert.Equal(t, expected, key)
	})

	t.Run("get missing key", func(t *testing.T) {
		keyName := "missingUserKey"
		key, ok := keys.GetDetailsByKey(keyName)
		assert.False(t, ok)

		expected := &apikeys.APIKeyDetailsFromFile{}
		assert.Equal(t, expected, key)
	})
}

func TestAPIKeysFromFile_Healthy(t *testing.T) {
	keyfile := filepath.Join("testdata", "apikeys.json")
	keys := apikeys.APIKeysFromFile{FileName: keyfile}

	t.Run("keys are available", func(t *testing.T) {
		err := keys.Healthy()
		assert.Nil(t, err)
	})

	t.Run("no keys in file", func(t *testing.T) {
		keys.FileName = filepath.Join("testdata", "empty.json")
		err := keys.Healthy()
		assert.Error(t, err)
	})

	t.Run("incorrect json filename", func(t *testing.T) {
		keys.FileName = ""
		err := keys.Healthy()
		assert.Error(t, err)
	})
}
