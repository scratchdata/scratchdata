package cache

import (
	"time"

	"github.com/patrickmn/go-cache"
)

// Cache implements the Cache interface using go-cache.
type Cache struct {
	cache *cache.Cache
}

// NewCache creates a new instance of Cache.
func NewCache() *Cache {
	return &Cache{
		cache: cache.New(5*time.Minute, 10*time.Minute), // default expiration and cleanup interval
	}
}

// Get retrieves a value from the cache for the given key.
func (c *Cache) Get(key string) ([]byte, bool) {
	if value, ok := c.cache.Get(key); ok {
		return value.([]byte), true
	}
	return nil, false
}

// Set sets a value in the cache for the given key with an optional expiration time.
func (c *Cache) Set(key string, value []byte, expires *time.Duration) error {
	if expires != nil {
		c.cache.Set(key, value, *expires)
	} else {
		c.cache.Set(key, value, cache.NoExpiration)
	}
	return nil
}
