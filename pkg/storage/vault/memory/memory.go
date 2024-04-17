package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/scratchdata/scratchdata/pkg/config"
)

type MemoryVault struct {
	destinations map[string]string
}

func NewMemoryVault(destinations []config.Destination) (*MemoryVault, error) {
	vault := &MemoryVault{
		destinations: make(map[string]string),
	}
	for _, dest := range destinations {
		destJSON, err := json.Marshal(dest.Settings)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal destination to JSON: %w", err)
		}
		vault.destinations[strconv.Itoa(int(dest.ID))] = string(destJSON)
	}
	return vault, nil
}

func (mv *MemoryVault) GetCredential(name string) (string, error) {
	dest, ok := mv.destinations[name]
	if !ok {
		return "", errors.New("credential not found")
	}
	return dest, nil
}

func (mv *MemoryVault) SetCredential(name string, value string) error {
	mv.destinations[name] = value
	return nil
}

func (mv *MemoryVault) DeleteCredential(name string) error {
	delete(mv.destinations, name)
	return nil
}
