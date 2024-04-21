// TODO: sqlite and postgres will probably have different sql queries
//       that we write outside of the ORM's auto-generated ones

package gorm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Gorm struct {
	DSN         string `mapstructure:"dsn"`
	DefaultUser string `mapstructure:"default_user"`
	db          *gorm.DB
}

func NewGorm(
	conf config.Database,
) (*Gorm, error) {
	rc := util.ConfigToStruct[Gorm](conf.Settings)
	var (
		db  *gorm.DB
		err error
	)
	switch conf.Type {
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(rc.DSN), &gorm.Config{})
	case "postgres":
		db, err = gorm.Open(postgres.Open(rc.DSN), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unknown database type: %s", conf.Type)
	}
	if err != nil {
		return nil, err
	}

	rc.db = db

	err = db.AutoMigrate(
		&models.ShareQuery{},
		&models.Team{},
		&models.User{},
		&models.Destination{},
		&models.APIKey{},
		&models.Message{},
		&models.ConnectionRequest{},
	)
	if err != nil {
		return nil, err
	}

	var teamCount int64
	db.Model(&models.Team{}).Count(&teamCount)

	return rc, nil
}

func (s *Gorm) VerifyAdminAPIKey(ctx context.Context, apiKey string) bool {
	return false
}

func (s *Gorm) CreateConnectionRequest(ctx context.Context, destID uint) (models.ConnectionRequest, error) {
	requestId := uuid.New().String()
	req := models.ConnectionRequest{
		RequestID:     requestId,
		DestinationID: destID,
		// TODO breadchris make configurable
		Expiration: time.Now().Add(time.Hour * 24 * 7),
	}

	res := s.db.Create(&req)
	if res.Error != nil {
		return models.ConnectionRequest{}, res.Error
	}
	return req, nil
}

func (s *Gorm) GetConnectionRequest(ctx context.Context, requestId uuid.UUID) (models.ConnectionRequest, error) {
	var req models.ConnectionRequest
	res := s.db.Preload("GetDestination").First(&req, "request_id = ?", requestId.String())
	if res.Error != nil {
		return models.ConnectionRequest{}, res.Error
	}
	return req, nil
}

func (s *Gorm) CreateShareQuery(
	ctx context.Context,
	destId uint,
	name,
	query string,
	expires time.Duration,
) (queryId uuid.UUID, err error) {
	id := uuid.New()
	link := models.ShareQuery{
		UUID:          id.String(),
		DestinationID: destId,
		Name:          name,
		Query:         query,
		ExpiresAt:     time.Now().Add(expires),
	}

	res := s.db.Create(&link)
	if res.Error != nil {
		return uuid.Nil, res.Error
	}

	return id, nil
}

func (s *Gorm) GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.ShareQuery, bool) {
	var query models.ShareQuery
	res := s.db.First(&query, "uuid = ? AND expires_at > ?", queryId.String(), time.Now())
	if res.Error != nil {
		if !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.Error().Err(res.Error).Str("query_id", queryId.String()).Msg("Unable to find shared query")
		}

		return models.ShareQuery{}, false
	}
	return query, true
}

func (s *Gorm) GetTeamId(userId uint) (uint, error) {
	var user models.User

	res := s.db.Preload("Teams").First(&user, userId)
	if res.Error != nil {
		return 0, res.Error
	}
	if len(user.Teams) == 0 {
		return 0, errors.New("user has no teams")
	}

	return user.Teams[0].ID, nil
}

func (s *Gorm) AddAPIKey(ctx context.Context, destId uint, hashedAPIKey string) error {
	a := models.APIKey{
		DestinationID: destId,
		HashedAPIKey:  hashedAPIKey,
	}

	if res := s.db.Create(&a); res.Error != nil {
		return res.Error
	}
	return nil
}

func (s *Gorm) CreateDestination(
	ctx context.Context,
	teamId uint,
	name string,
	destType string,
) (models.Destination, error) {
	// TODO breadchris what fields are considered unique?

	dest := models.Destination{
		TeamID: teamId,
		Name:   name,
		Type:   destType,
	}

	result := s.db.Create(&dest)
	if result.Error != nil {
		return models.Destination{}, result.Error
	}

	if result.RowsAffected != 1 {
		return models.Destination{}, errors.New("unable to create destination")
	}

	return dest, nil
}

func (s *Gorm) DeleteConnectionRequest(ctx context.Context, id uint) error {
	res := s.db.Delete(&models.ConnectionRequest{}, "id = ?", id)
	return res.Error
}

func (s *Gorm) UpdateDestination(ctx context.Context, dest models.Destination) error {
	res := s.db.Save(&dest)
	return res.Error
}

func (s *Gorm) CreateTeam(name string) (*models.Team, error) {
	team := &models.Team{
		Name: name,
	}

	res := s.db.Create(team)
	if res.Error != nil {
		return nil, res.Error
	}

	return team, nil
}

func (s *Gorm) AddUserToTeam(userId uint, teamId uint) error {
	user := s.GetUser(userId)
	if user == nil {
		return errors.New("user not found")
	}

	t := models.Team{
		Model: gorm.Model{
			ID: teamId,
		},
	}
	res := s.db.Model(user).Association("Teams").Append([]models.Team{t})
	return res
}

func (s *Gorm) GetDestinations(c context.Context, userId uint) ([]models.Destination, error) {
	teamId, err := s.GetTeamId(userId)
	if err != nil {
		return nil, err
	}

	var destinations []models.Destination
	res := s.db.Where("team_id = ?", teamId).Find(&destinations)
	if res.Error != nil {
		return nil, res.Error
	}
	return destinations, nil
}

func (s *Gorm) GetDestination(c context.Context, teamId, destId uint) (models.Destination, error) {
	var dest models.Destination
	res := s.db.First(&dest, "team_id = ? AND id = ?", teamId, destId)
	if res.Error != nil {
		return dest, res.Error
	}
	return dest, nil
}

func (s *Gorm) DeleteDestination(ctx context.Context, teamId uint, destId uint) error {
	res := s.db.Delete(&models.Destination{}, "team_id = ? AND id = ?", teamId, destId)
	return res.Error
}

func (s *Gorm) Hash(str string) string {
	hash := sha256.Sum256([]byte(str))
	return hex.EncodeToString(hash[:])
}

func (s *Gorm) GetUser(userId uint) *models.User {
	var user models.User
	tx := s.db.First(&user, userId)
	if tx.Error != nil {
		log.Error().Err(tx.Error).Msg("Unable to get user")
	}
	return &user
}

func (s *Gorm) CreateUser(email string, source string, details string) (*models.User, error) {
	user := &models.User{
		Email:       email,
		AuthType:    source,
		AuthDetails: details,
	}

	res := s.db.Transaction(func(tx *gorm.DB) error {
		result := tx.Where(models.User{Email: email, AuthType: source}).FirstOrCreate(&user)
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 1 {
			team := &models.Team{Name: email, Users: []*models.User{user}}
			result = tx.Create(team)
			if result.Error != nil {
				return result.Error
			}
		}

		return nil
	})

	// result := db.db.Where(User{Email: email, AuthType: source}).FirstOrCreate(&user)
	return user, res
}

func (s *Gorm) GetAPIKeyDetails(ctx context.Context, hashedKey string) (models.APIKey, error) {
	var dbKey models.APIKey

	tx := s.db.Joins("Destination.Team").First(&dbKey, "hashed_api_key = ?", hashedKey)
	if tx.RowsAffected == 0 {
		return models.APIKey{}, errors.New("api key not found")
	}

	return dbKey, nil
}

func (s *Gorm) GetDestinationByID(ctx context.Context, destinationId uint) (models.Destination, error) {
	var dbDest models.Destination

	tx := s.db.First(&dbDest, destinationId)
	if tx.Error != nil {
		return dbDest, tx.Error
	}
	return dbDest, nil
}
