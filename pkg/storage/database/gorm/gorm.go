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

	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/models"
	"github.com/scratchdata/scratchdata/pkg/util"
	"gorm.io/datatypes"
	"gorm.io/driver/postgres"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
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
		&models.ShareLink{},
		&models.Team{},
		&models.User{},
		&models.Destination{},
		&models.APIKey{},
		&models.Message{},
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

func (s *Gorm) CreateShareQuery(ctx context.Context, destId int64, query string, expires time.Duration) (queryId uuid.UUID, err error) {
	id := uuid.New()
	link := models.ShareLink{
		UUID:          id.String(),
		DestinationID: destId,
		Query:         query,
		ExpiresAt:     time.Now().Add(expires),
	}

	res := s.db.Create(&link)
	if res.Error != nil {
		return uuid.Nil, res.Error
	}

	return id, nil
}

func (s *Gorm) GetShareQuery(ctx context.Context, queryId uuid.UUID) (models.SharedQuery, bool) {
	var link models.ShareLink
	res := s.db.First(&link, "uuid = ? AND expires_at > ?", queryId.String(), time.Now())
	if res.Error != nil {
		if !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			log.Error().Err(res.Error).Str("query_id", queryId.String()).Msg("Unable to find shared query")
		}

		return models.SharedQuery{}, false
	}

	rc := models.SharedQuery{
		ID:            link.UUID,
		Query:         link.Query,
		ExpiresAt:     link.ExpiresAt,
		DestinationID: link.DestinationID,
	}

	return rc, true
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

func (s *Gorm) AddAPIKey(ctx context.Context, destId int64, hashedAPIKey string) error {
	a := models.APIKey{
		DestinationID: uint(destId),
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
	settings map[string]any,
) (config.Destination, error) {
	// TODO breadchris what fields are considered unique?

	dest := &models.Destination{
		TeamID:   teamId,
		Name:     name,
		Type:     destType,
		Settings: datatypes.NewJSONType(settings),
	}

	res := s.db.Create(dest)
	if res.Error != nil {
		return config.Destination{}, res.Error
	}
	return config.Destination{
		ID:       int64(dest.ID),
		Name:     name,
		Type:     destType,
		Settings: settings,
	}, nil
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

func (s *Gorm) DeleteDestination(ctx context.Context, teamId uint, destId int64) error {
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

	tx := s.db.First(&dbKey, "hashed_api_key = ?", hashedKey)
	if tx.RowsAffected == 0 {
		return models.APIKey{}, errors.New("api key not found")
	}

	return dbKey, nil
}

func (s *Gorm) GetDestinationCredentials(ctx context.Context, destinationId int64) (models.Destination, error) {
	var dbDest models.Destination

	tx := s.db.First(&dbDest, destinationId)
	if tx.Error != nil {
		return dbDest, tx.Error
	}
	return dbDest, nil
}
