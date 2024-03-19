package vault

// Vault defines the interface for managing credentials.
type Vault interface {
	GetCredential(name string) string
	SetCredential(name, value string)
}
