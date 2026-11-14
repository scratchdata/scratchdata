package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/scratchdata/scratchdata/pkg/config"
)

var ErrSecretNotFound = errors.New("secret not found in memory vault")

type Vault struct {
	mu    sync.RWMutex
	items map[string]string
}

func NewVault(conf map[string]any, destinations []config.Destination) (*Vault, error) {
	v := &Vault{
		items: make(map[string]string),
	}

	for idx, dest := range destinations {
		if len(dest.Settings) == 0 {
			continue
		}
		raw, err := json.Marshal(dest.Settings)
		if err != nil {
			continue
		}
		val := string(raw)
		if dest.Name != "" {
			v.items[dest.Name] = val
		}
		if dest.ID != 0 {
			v.items[fmt.Sprintf("%d", dest.ID)] = val
			v.items[fmt.Sprintf("destination_%d", dest.ID)] = val
		}
		seqID := idx + 1
		if _, exists := v.items[fmt.Sprintf("%d", seqID)]; !exists {
			v.items[fmt.Sprintf("%d", seqID)] = val
			v.items[fmt.Sprintf("destination_%d", seqID)] = val
		}
	}

	return v, nil
}

func (v *Vault) GetCredential(name string) string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.items[name]
}

func (v *Vault) SetCredential(name string, value string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.items[name] = value
	return nil
}

func (v *Vault) DeleteCredential(name string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.items, name)
	return nil
}

func (v *Vault) Store(key string, value []byte) error {
	return v.SetCredential(key, string(value))
}

func (v *Vault) Retrieve(key string) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	val, ok := v.items[key]
	if !ok {
		return nil, ErrSecretNotFound
	}
	return []byte(val), nil
}

func (v *Vault) Delete(key string) error {
	return v.DeleteCredential(key)
}

func (v *Vault) GetCredentials(name string) (map[string]any, error) {
	raw, err := v.Retrieve(name)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (v *Vault) SetCredentials(name string, value map[string]any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return v.Store(name, raw)
}
