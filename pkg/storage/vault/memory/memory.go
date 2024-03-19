package memory

import (
	"errors"

	"github.com/scratchdata/scratchdata/config"
)

type MemoryVault struct {
	destinations map[string]config.Destination
}

func NewMemoryVault(destinations []config.Destination) (*MemoryVault, error) {
	vault := &MemoryVault{
		destinations: make(map[string]config.Destination),
	}
	for _, dest := range destinations {
		vault.destinations[dest.Name] = dest
	}
	return vault, nil
}

func (mv *MemoryVault) GetCredential(name string) (config.Destination, error) {
	dest, ok := mv.destinations[name]
	if !ok {
		return config.Destination{}, errors.New("credential not found")
	}

	if len(dest.APIKeys) > 0 {
		return dest, nil
	}

	return config.Destination{}, errors.New("credential not found")
}

func (mv *MemoryVault) SetCredential(name string, value config.Destination) error {
	// Check if the destination already exists
	_, ok := mv.destinations[name]
	if ok {
		return errors.New("destination already exists")
	}

	// Set the value directly to destinations[name]
	mv.destinations[name] = value

	return nil
}
