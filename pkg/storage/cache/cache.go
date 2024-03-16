package cache
 
import (
	"time"

	"github.com/scratchdata/scratchdata/config"
	"github.com/scratchdata/scratchdata/pkg/storage/cache/memory"
)

type Cache interface {
	Get(key string) (value []byte, ok bool)
	Set(key string, value []byte, expires *time.Duration) error
}

func NewCache(conf config.Cache) (Cache, error) {
	switch conf.Type {
	case "memory":
		return memory.NewCache(conf.Settings)
	}

	return nil, nil
}