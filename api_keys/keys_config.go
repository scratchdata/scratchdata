package apikeys

type APIKeysFromConfig struct {
	Users map[string]string
}

func (k *APIKeysFromConfig) GetDetailsByKey(key string) (APIKeyDetails, bool) {
	user, ok := k.Users[key]
	return &APIKeyDetailsFromConfig{
		user: user,
	}, ok
}
func (k *APIKeysFromConfig) CreateKey(APIKeyDetails) (APIKeyDetails, error) {
	return &APIKeyDetailsFromConfig{}, nil
}
func (k *APIKeysFromConfig) DeleteKey(key string) error {
	return nil
}

type APIKeyDetailsFromConfig struct {
	user string
}

func (k *APIKeyDetailsFromConfig) GetDBUser() string {
	return k.user
}
func (k *APIKeyDetailsFromConfig) GetPermissions() APIKeyPermissions {
	return APIKeyPermissions{}
}
