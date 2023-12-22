package static

import (
	"errors"
	"scratchdata/models"

	"github.com/BurntSushi/toml"
)

type StaticDB struct {
	Filename string `mapstructure:"filename"`

	conf staticConfig
}

type staticConfig struct {
	Accounts            []models.Account            `toml:"accounts"`
	Users               []models.User               `toml:"users"`
	ApiKeys             []models.APIKey             `toml:"api_keys"`
	DatabaseConnections []models.DatabaseConnection `toml:"database_connections"`
}

func (d *StaticDB) Open() error {
	_, err := toml.DecodeFile(d.Filename, &d.conf)
	if err != nil {
		return err
	}

	return nil
}

func (d *StaticDB) Close() error {
	return nil
}

func (d *StaticDB) Hash(input string) string {
	return input
}

func (d *StaticDB) HealthCheck() error {
	if d.conf.ApiKeys == nil {
		return errors.New("ApiKeys is null")
	}
	if len(d.conf.ApiKeys) == 0 {
		return errors.New("ApiKeys is empty")
	}
	if d.conf.Users == nil {
		return errors.New("Users is null")
	}
	if len(d.conf.Users) == 0 {
		return errors.New("Users is empty")
	}
	return nil
}

func (d *StaticDB) GetAPIKeyDetails(hashedKey string) models.APIKey {
	for _, apiKey := range d.conf.ApiKeys {
		if apiKey.HashedAPIKey == hashedKey {
			return apiKey
		}
	}
	return models.APIKey{}
}

func (d *StaticDB) GetAccount(id string) models.Account {
	for _, account := range d.conf.Accounts {
		if account.ID == id {
			return account
		}
	}
	return models.Account{}
}

func (d *StaticDB) GetDatabaseConnections(accountID string) []models.DatabaseConnection {
	rc := []models.DatabaseConnection{}
	for _, conn := range d.conf.DatabaseConnections {
		if conn.AccountID == accountID {
			rc = append(rc, conn)
		}
	}
	return rc
}

func (d *StaticDB) GetDatabaseConnection(connectionID string) models.DatabaseConnection {
	for _, conn := range d.conf.DatabaseConnections {
		if conn.ID == connectionID {
			return conn
		}
	}
	return models.DatabaseConnection{}
}
