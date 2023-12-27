package util

import (
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog/log"
)

// Takes an arbitrary map and populates a struct with the fields.
// Used for transforming configuration files into structs.
func ConfigToStruct[T any](rawConfig map[string]interface{}) *T {
	config := new(T)
	if err := mapstructure.Decode(rawConfig, config); err != nil {
		log.Error().Msgf("Error decoding config: %v", err)
	}
	return config
}
