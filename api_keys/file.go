package apikeys

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"
)

type APIKeysFromFile struct {
	FileName    string
	users       map[string]string
	lastUpdated time.Time
	mu          sync.Mutex
}

func (k *APIKeysFromFile) readCSVToMap() (map[string]string, error) {
	// Open the CSV file
	file, err := os.Open(k.FileName)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	// Create a CSV reader
	reader := csv.NewReader(file)

	// Read all the records from the CSV
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %v", err)
	}

	apiMap := make(map[string]string)

	// Skip the header and populate the map
	for _, record := range records {
		if len(record) < 2 {
			continue // Skip records with insufficient fields
		}
		apiKey := record[0]
		user := record[1]
		apiMap[apiKey] = user
	}

	return apiMap, nil
}

func (k *APIKeysFromFile) GetDetailsByKey(key string) (APIKeyDetails, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.users == nil || time.Since(k.lastUpdated) > 30*time.Second {
		users, ok := k.readCSVToMap()
		if ok == nil {
			k.users = users
		}
	}

	user, ok := k.users[key]
	return &APIKeyDetailsFromFile{
		user: user,
	}, ok
}

func (k *APIKeysFromFile) CreateKey(APIKeyDetails) (APIKeyDetails, error) {
	return &APIKeyDetailsFromFile{}, nil
}

func (k *APIKeysFromFile) DeleteKey(key string) error {
	return nil
}

type APIKeyDetailsFromFile struct {
	user string
}

func (k *APIKeyDetailsFromFile) GetDBUser() string {
	return k.user
}
func (k *APIKeyDetailsFromFile) GetPermissions() APIKeyPermissions {
	return APIKeyPermissions{}
}
