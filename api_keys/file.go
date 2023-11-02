package apikeys

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type APIKeysFromFile struct {
	FileName    string
	users       map[string]APIKeyDetailsFromFile
	lastUpdated time.Time
	mu          sync.Mutex
}

type APIKeyDetailsFromFile struct {
	Name       string `json:"name"`
	APIKey     string `json:"api_key"`
	DBCluster  string `json:"db_cluster"`
	DBName     string `json:"db_name"`
	DBUser     string `json:"db_user"`
	DBPassword string `json:"db_password"`
}

func (k *APIKeysFromFile) readJSONToMap() (map[string]APIKeyDetailsFromFile, error) {
	// Open the CSV file
	file, err := os.Open(k.FileName)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %v", err)
	}

	var apiKeyDetails []APIKeyDetailsFromFile
	err = json.Unmarshal(byteValue, &apiKeyDetails)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling json: %v", err)
	}

	apiMap := make(map[string]APIKeyDetailsFromFile)

	for _, detail := range apiKeyDetails {
		apiMap[detail.APIKey] = detail
	}

	return apiMap, nil
}

func (k *APIKeysFromFile) Healthy() error {
	// For this node to be considered healthy, there must be active API keys
	// TODO: should we check the actual value of k.users instead?

	data, err := k.readJSONToMap()
	if err != nil {
		return err
	}
	if data == nil {
		return errors.New("Unable to create map from JSON")
	}
	if len(data) == 0 {
		return errors.New("There are no users in the file")
	}

	return nil
}

func (k *APIKeysFromFile) GetDetailsByKey(key string) (APIKeyDetails, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.users == nil || time.Since(k.lastUpdated) > 30*time.Second {
		users, err := k.readJSONToMap()
		if err == nil {
			k.users = users
			k.lastUpdated = time.Now()
		} else {
			log.Println(err)
		}
	}

	user, ok := k.users[key]
	return &user, ok
}

func (k *APIKeysFromFile) CreateKey(APIKeyDetails) (APIKeyDetails, error) {
	return &APIKeyDetailsFromFile{}, nil
}

func (k *APIKeysFromFile) DeleteKey(key string) error {
	return nil
}

func (k *APIKeyDetailsFromFile) GetName() string {
	return k.Name
}

func (k *APIKeyDetailsFromFile) GetDBCluster() string {
	return k.DBCluster
}

func (k *APIKeyDetailsFromFile) GetDBName() string {
	return k.DBName
}

func (k *APIKeyDetailsFromFile) GetDBUser() string {
	return k.DBUser
}

func (k *APIKeyDetailsFromFile) GetDBPassword() string {
	return k.DBPassword
}

func (k *APIKeyDetailsFromFile) GetPermissions() APIKeyPermissions {
	return APIKeyPermissions{}
}
