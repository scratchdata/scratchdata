package static

import (
	"context"

	"github.com/mitchellh/mapstructure"
	"github.com/scratchdata/scratchdata/pkg/config"
	"github.com/scratchdata/scratchdata/pkg/storage/database/gorm"
)

func NewStaticDatabase(conf config.Database, destinations []config.Destination, apiKeys []config.APIKey) (*gorm.Gorm, error) {
	ctx := context.TODO()

	defaultSettings := gorm.Gorm{DSN: "file::memory:?cache=shared"}
	defaultSettingsMap := map[string]any{}
	mapstructure.Decode(&defaultSettings, &defaultSettingsMap)

	gormConf := config.Database{
		Type:     "sqlite",
		Settings: defaultSettingsMap,
	}

	rc, err := gorm.NewGorm(gormConf)
	if err != nil {
		return nil, err
	}

	team, err := rc.CreateTeam("Team Scratch")
	if err != nil {
		return nil, err
	}

	user, err := rc.CreateUser("scratch@example.com", "static", "")
	if err != nil {
		return nil, err
	}

	rc.AddUserToTeam(user.ID, team.ID)

	for _, d := range destinations {
		dest, err := rc.CreateDestination(ctx, team.ID, d.Name, d.Type)
		if err != nil {
			return nil, err
		}

		for _, apiKey := range d.APIKeys {
			err = rc.AddAPIKey(ctx, dest.ID, rc.Hash(apiKey))
			if err != nil {
				return nil, err
			}
		}
	}

	return rc, nil

}
