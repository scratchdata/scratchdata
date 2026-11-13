package memory

import (
	"encoding/json"
	"strconv"
	"sync"

	"github.com/scratchdata/scratchdata/pkg/config"
)

type Vault struct {
	mu    sync.RWMutex
	items map[string]string
}

func (v *Vault) GetCredential(name string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.items[name]
}

func (v *Vault) SetCredential(name, value string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.items[name] = value
}

// NewVault returns a new Vault backed by the destinations defined in the YAML config
func NewVault(destinations []config.Destination) (*Vault, error) {
	rc := &Vault{
		items: map[string]string{},
	}

	for i, destination := range destinations {
		id := destination.ID
		if id == 0 {
			id = int64(i + 1)
		}
		data, err := json.Marshal(destination)
		if err != nil {
			return nil, err
		}
		rc.items[strconv.FormatInt(id, 10)] = string(data)
	}

	return rc, nil
}
