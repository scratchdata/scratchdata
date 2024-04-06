package gorm

import (
	"context"
	"sync"
	"time"

	gocache_store "github.com/eko/gocache/store/go_cache/v4"
	"github.com/go-gorm/caches/v4"
	gocache "github.com/patrickmn/go-cache"
)

type memoryCacher struct {
	store *sync.Map

	gcstore *gocache_store.GoCacheStore
}

func newMemoryCacher() *memoryCacher {
	gocacheClient := gocache.New(1*time.Minute, 5*time.Minute)
	gocacheStore := gocache_store.NewGoCache(gocacheClient)

	rc := memoryCacher{
		gcstore: gocacheStore,
	}
	return &rc
}

func (c *memoryCacher) init() {
	// if c.store == nil {
	// 	c.store = &sync.Map{}
	// }
}

func (c *memoryCacher) Get(ctx context.Context, key string, q *caches.Query[any]) (*caches.Query[any], error) {
	value, err := c.gcstore.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if err := q.Unmarshal(value.([]byte)); err != nil {
		return nil, err
	}

	return q, nil
}

func (c *memoryCacher) Store(ctx context.Context, key string, val *caches.Query[any]) error {
	res, err := val.Marshal()
	if err != nil {
		return err
	}

	return c.gcstore.Set(ctx, key, res)
}

func (c *memoryCacher) Invalidate(ctx context.Context) error {
	return c.gcstore.Clear(ctx)
}
