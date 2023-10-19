package apikeys

type APIKeys interface {
	GetDetailsByKey(key string) (APIKeyDetails, bool)
	CreateKey(APIKeyDetails) (APIKeyDetails, error)
	DeleteKey(key string) error
}

type APIKeyDetails interface {
	GetDBUser() string
	GetPermissions() APIKeyPermissions
}

type APIKeyPermissions struct {
	// User      string
	// CanRead   bool
	// CanWrite  bool
	// CanAdmin  bool
	// Databases []string
	// Tables    []string
}
