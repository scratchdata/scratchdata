package vault

import "errors"

// Vault interface defines methods for retrieving and setting credentials
type Vault interface {
	GetCredential(name string) (string, error)
	SetCredential(name, value string) error
}

// MemoryVault is a default implementation that retrieves credentials from config
type MemoryVault struct {
	config map[string]interface{}
}

// GetCredential retrieves a credential from the config
func (m *MemoryVault) GetCredential(name string) (string, error) {
	value, ok := m.config[name]
	if !ok {
		return "", errors.New("credential not found")
	}

	strVal, ok := value.(string)
	if !ok {
		return "", errors.New("credential is not a string")
	}
	return strVal, nil
}
