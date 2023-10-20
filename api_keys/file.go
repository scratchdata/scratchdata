package apikeys

import (
	"encoding/csv"
	"fmt"
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

func (k *APIKeysFromFile) readCSVToMap() (map[string]APIKeyDetailsFromFile, error) {
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

	apiMap := make(map[string]APIKeyDetailsFromFile)

	// Skip the header and populate the map
	for i, record := range records {

		if i == 0 {
			continue
		}

		if len(record) < 3 {
			continue // Skip records with insufficient fields
		}
		apiKey := record[0]
		user := record[1]
		dbPass := record[2]
		apiMap[apiKey] = APIKeyDetailsFromFile{user: user, password: dbPass}
	}

	return apiMap, nil
}

func (k *APIKeysFromFile) GetDetailsByKey(key string) (APIKeyDetails, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.users == nil || time.Since(k.lastUpdated) > 30*time.Second {
		users, err := k.readCSVToMap()
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

type APIKeyDetailsFromFile struct {
	user     string
	password string
}

func (k *APIKeyDetailsFromFile) GetDBUser() string {
	return k.user
}

func (k *APIKeyDetailsFromFile) GetDBPassword() string {
	return k.password
}

func (k *APIKeyDetailsFromFile) GetPermissions() APIKeyPermissions {
	return APIKeyPermissions{}
}
