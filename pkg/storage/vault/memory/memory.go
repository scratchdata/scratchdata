package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/scratchdata/scratchdata/pkg/config"
)

type MemoryVault struct {
	destinations map[string]map[string]any
}

func NewMemoryVault(destinations []config.Destination) (*MemoryVault, error) {
	vault := &MemoryVault{
		destinations: make(map[string]map[string]any),
	}
	for _, dest := range destinations {
		vault.destinations[strconv.Itoa(int(dest.ID))] = dest.Settings
	}
	return vault, nil
}

func (mv *MemoryVault) GetCredential(name string) (string, error) {
	dest, ok := mv.destinations[name]
	if !ok {
		return "", errors.New("credential not found")
	}

	destJSON, err := json.Marshal(dest)
	if err != nil {
		return "", fmt.Errorf("failed to marshal destination to JSON: %w", err)
	}

	return string(destJSON), nil
}

func (mv *MemoryVault) SetCredential(name string, value string) error {
	return nil
}
