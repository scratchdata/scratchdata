package accounts

import (
	"scratchdata/pkg/accounts/dummy"
	"scratchdata/util"
)

func GetAccountManager(config map[string]interface{}) AccountManager {
	configType := config["type"]

	switch configType {
	case "dummy":
		return util.ConfigToStruct[*dummy.DummyAccountManager](config)
	default:
		return nil
	}
}
