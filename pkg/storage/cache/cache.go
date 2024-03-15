package cache

import "time"

type Cache interface {
	Get(key string) (value []byte, ok bool)
	Set(key string, value []byte, expires *time.Duration) error
}
