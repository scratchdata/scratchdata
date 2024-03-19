package memory

import (
	"github.com/scratchdata/scratchdata/config"
)

// MemoryVault implements Vault interface.
type MemoryVault struct {
	destinations []config.Destination
}

// NewMemoryVault creates a new instance of MemoryVault.
func NewMemoryVault(destinations []config.Destination) *MemoryVault {
	return &MemoryVault{destinations: destinations}
}

// GetCredential retrieves a credential from memory vault.
func (mv *MemoryVault) GetCredential(name string) string {
	for _, dest := range mv.destinations {
		if dest.Name == name {
			// Assuming the first API key is used as the credential
			if len(dest.APIKeys) > 0 {
				return dest.APIKeys[0]
			}
		}
	}
	return "" // Return empty string if credential not found
}

// SetCredential does nothing for MemoryVault.
func (mv *MemoryVault) SetCredential(name, value string) {
	// MemoryVault does not support setting credentials dynamically
}
