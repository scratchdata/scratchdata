package static

import (
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

func (d *StaticDB) GetAccount(id string) models.Account {
	return d.conf.Accounts[0]
}

func (d *StaticDB) GetUsers(accountID string) []models.User {
	return d.conf.Users
}

func (d *StaticDB) GetAPIKeys(accountID string) []models.APIKey {
	return d.conf.ApiKeys
}

func (d *StaticDB) GetDatabaseConnections(accountID string) []models.DatabaseConnection {
	return d.conf.DatabaseConnections
}
