package static

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
)

var StaticDBError = errors.New("Cannot make changes when using static config. Update config file or use a database instead.")

type StaticDatabase struct {
	conf                config.Database
	destinations        []config.Destination
	apiKeyToDestination map[string]uint
	adminAPIKeys        []config.APIKey

	ids   uint
	mu    sync.Mutex
	queue map[models.MessageType][]*models.Message
}

func (db *StaticDatabase) DeleteDestination(ctx context.Context, userId uint, destId int64) error {
	return StaticDBError
}

func NewStaticDatabase(conf config.Database, destinations []config.Destination, apiKeys []config.APIKey) (*StaticDatabase, error) {
	rc := StaticDatabase{
		conf:                conf,
		destinations:        destinations,
		apiKeyToDestination: map[string]uint{},
		adminAPIKeys:        apiKeys,

		queue: make(map[models.MessageType][]*models.Message),
	}

	for i, destination := range destinations {
		for _, apiKey := range destination.APIKeys {
			rc.apiKeyToDestination[apiKey] = uint(i)
		}
	}

	return &rc, nil
}

func (db *StaticDatabase) Hash(s string) string {
	return s
}

func (db *StaticDatabase) GetDestinations(ctx context.Context, teamId uint) ([]config.Destination, error) {
	return db.destinations, nil
}

func (db *StaticDatabase) AddAPIKey(ctx context.Context, destId int64, key string) error {
	return StaticDBError
}

func (db *StaticDatabase) CreateDestination(ctx context.Context, teamId uint, name string, destType string, settings map[string]any) (config.Destination, error) {
	return config.Destination{}, StaticDBError
}

func (db *StaticDatabase) VerifyAdminAPIKey(ctx context.Context, apiKey string) bool {
	for _, key := range db.adminAPIKeys {
		if key.Key == apiKey {
			return true
		}
	}

	return false
}

func (db *StaticDatabase) CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error) {
	return uuid.Nil, StaticDBError
}

func (db *StaticDatabase) GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool) {
	return models.SharedQuery{}, false
}

func (db *StaticDatabase) GetAPIKeyDetails(ctx context.Context, apiKey string) (models.APIKey, error) {
	dbId, ok := db.apiKeyToDestination[apiKey]
	if !ok {
		return models.APIKey{}, errors.New("invalid API key")
	}
	rc := models.APIKey{
		DestinationID: dbId,
	}
	return rc, nil
}

func (db *StaticDatabase) GetDestinationCredentials(ctx context.Context, dbID int64) (config.Destination, error) {
	return db.destinations[dbID], nil
}

func (db *StaticDatabase) CreateUser(email string, source string, details string) (*models.User, error) {
	user := &models.User{
		Email:    "scratchdata@example.com",
		AuthType: "static",
	}
	user.ID = 1
	return user, nil
}

func (db *StaticDatabase) GetUser(int64) *models.User {
	user := &models.User{
		Email:    "scratchdata@example.com",
		AuthType: "static",
	}
	user.ID = 1
	return user
}

func (db *StaticDatabase) Enqueue(messageType models.MessageType, m any) (*models.Message, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	queue, ok := db.queue[messageType]
	if !ok {
		queue = make([]*models.Message, 0)
		db.queue[messageType] = queue
	}

	mStr, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	message := &models.Message{
		MessageType: messageType,
		Status:      models.New,
		Message:     string(mStr),
	}

	db.ids++
	message.ID = db.ids

	queue = append(queue, message)
	db.queue[messageType] = queue

	return message, nil
}

func (db *StaticDatabase) Dequeue(messageType models.MessageType, claimedBy string) (*models.Message, bool) {
	db.mu.Lock()
	defer db.mu.Unlock()

	queue, ok := db.queue[messageType]
	if !ok {
		return nil, false
	}

	if len(queue) == 0 {
		return nil, false
	}

	for _, message := range queue {
		if message.Status == models.Claimed {
			continue
		}
		message.ClaimedAt = time.Now()
		message.ClaimedBy = claimedBy
		message.Status = models.Claimed

		return message, true
	}

	return nil, false
}

func (db *StaticDatabase) Delete(id uint) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for k, queue := range db.queue {
		found := -1
		for i, message := range queue {
			if message.ID == id {
				found = i
				break
			}
		}

		if found >= 0 {
			newQueue := append(queue[:found], queue[found+1:]...)
			db.queue[k] = newQueue
			break
		}
	}

	return nil
}
