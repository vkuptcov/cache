package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/go-redis/redis/v8"
	"golang.org/x/sync/singleflight"
)

const timeLen = 4

var ErrCacheMiss = errors.New("cache: key is missing")
var errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")

type rediser interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd

	Get(ctx context.Context, key string) *redis.StringCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd

	Pipeline() redis.Pipeliner
}

type Item struct {
	Ctx context.Context

	Key   string
	Value interface{}

	// TTL is the cache expiration time.
	// Default TTL is taken from Options
	TTL time.Duration

	// Do returns value to be cached.
	Do func(*Item) (interface{}, error)

	// IfExists only sets the key if it already exist.
	IfExists bool

	// IfNotExists only sets the key if it does not already exist.
	IfNotExists bool

	// SkipLocalCache skips local cache as if it is not set.
	SkipLocalCache bool
}

func (item *Item) Context() context.Context {
	if item.Ctx == nil {
		return context.Background()
	}
	return item.Ctx
}

func (item *Item) value() (interface{}, error) {
	if item.Do != nil {
		return item.Do(item)
	}
	if item.Value != nil {
		return item.Value, nil
	}
	return nil, nil
}

//------------------------------------------------------------------------------

type Options struct {
	Redis rediser

	LocalCache    *fastcache.Cache
	LocalCacheTTL time.Duration

	// RedisCacheDefaultTTL is the cache expiration time.
	// 1 hour by default
	RedisCacheDefaultTTL time.Duration

	StatsEnabled bool

	Marshaller Marshaller
}

type Marshaller interface {
	Marshal(value interface{}) ([]byte, error)
	Unmarshal(b []byte, value interface{}) error
}

func (opt *Options) init() {
	initDuration := func(current, defDur time.Duration) time.Duration {
		if current < 0 {
			return 0
		} else if current == 0 {
			return defDur
		}
		return current
	}
	opt.LocalCacheTTL = initDuration(opt.LocalCacheTTL, time.Minute)
	opt.RedisCacheDefaultTTL = initDuration(opt.RedisCacheDefaultTTL, time.Hour)
	if opt.Marshaller == nil {
		opt.Marshaller = msgpackMarshaller{}
	}
}

type Cache struct {
	opt *Options

	group singleflight.Group

	hits   uint64
	misses uint64
}

func New(opt *Options) *Cache {
	opt.init()
	return &Cache{
		opt: opt,
	}
}

// Set caches the item.
func (cd *Cache) Set(item *Item) error {
	_, _, err := cd.set(cd.opt.Redis, item)
	return err
}

// MSet sets multiple elements
// @todo unify with Set
func (cd *Cache) MSet(ctx context.Context, items ...*Item) (err error) {
	r := cd.opt.Redis
	var pipeliner redis.Pipeliner
	if len(items) > 1 {
		pipeliner = cd.opt.Redis.Pipeline()
		r = cd.opt.Redis.Pipeline()
	}
	for _, item := range items {
		_, _, err = cd.set(r, item)
		if err != nil {
			return err
		}
	}
	if pipeliner != nil {
		_, err = pipeliner.Exec(ctx)
	}
	return err
}

func (cd *Cache) set(redis rediser, item *Item) ([]byte, bool, error) {
	value, err := item.value()
	if err != nil {
		return nil, false, err
	}

	b, err := cd.Marshal(value)
	if err != nil {
		return nil, false, err
	}

	if cd.opt.LocalCache != nil {
		cd.localSet(item.Key, b)
	}

	if redis == nil {
		if cd.opt.LocalCache == nil {
			return b, true, errRedisLocalCacheNil
		}
		return b, true, nil
	}

	if item.IfExists {
		return b, true, redis.SetXX(item.Context(), item.Key, b, cd.redisTTL(item)).Err()
	}

	if item.IfNotExists {
		return b, true, redis.SetNX(item.Context(), item.Key, b, cd.redisTTL(item)).Err()
	}

	return b, true, redis.Set(item.Context(), item.Key, b, cd.redisTTL(item)).Err()
}

func (cd *Cache) redisTTL(item *Item) time.Duration {
	if item.TTL < 0 {
		return 0
	}
	if item.TTL < time.Second {
		return cd.opt.RedisCacheDefaultTTL
	}
	return item.TTL
}

// Exists reports whether value for the given key exists.
func (cd *Cache) Exists(ctx context.Context, key string) bool {
	return cd.Get(ctx, key, nil) == nil
}

// Get gets the value for the given key.
func (cd *Cache) Get(ctx context.Context, key string, value interface{}) error {
	return cd.get(ctx, key, value, false)
}

// Get gets the value for the given key skipping local cache.
func (cd *Cache) GetSkippingLocalCache(
	ctx context.Context, key string, value interface{},
) error {
	return cd.get(ctx, key, value, true)
}

func (cd *Cache) get(
	ctx context.Context,
	key string,
	value interface{},
	skipLocalCache bool,
) error {
	b, err := cd.getBytes(ctx, key, skipLocalCache)
	if err != nil {
		return err
	}
	return cd.Unmarshal(b, value)
}

func (cd *Cache) getBytes(ctx context.Context, key string, skipLocalCache bool) ([]byte, error) {
	if !skipLocalCache && cd.opt.LocalCache != nil {
		b, ok := cd.localGet(key)
		if ok {
			return b, nil
		}
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := cd.opt.Redis.Get(ctx, key).Bytes()
	if err != nil {
		if cd.opt.StatsEnabled {
			atomic.AddUint64(&cd.misses, 1)
		}
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, err
	}

	if cd.opt.StatsEnabled {
		atomic.AddUint64(&cd.hits, 1)
	}

	if !skipLocalCache && cd.opt.LocalCache != nil {
		cd.localSet(key, b)
	}
	return b, nil
}

// Once gets the item.Value for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Cache) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Value == nil || len(b) == 0 {
		return nil
	}

	if err := cd.Unmarshal(b, item.Value); err != nil {
		if cached {
			_ = cd.Delete(item.Context(), item.Key)
			return cd.Once(item)
		}
		return err
	}

	return nil
}

func (cd *Cache) getSetItemBytesOnce(item *Item) (b []byte, cached bool, err error) {
	if cd.opt.LocalCache != nil {
		b, ok := cd.localGet(item.Key)
		if ok {
			return b, true, nil
		}
	}

	v, err, _ := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getBytes(item.Context(), item.Key, item.SkipLocalCache)
		if err == nil {
			cached = true
			return b, nil
		}

		b, ok, err := cd.set(cd.opt.Redis, item)
		if ok {
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return v.([]byte), cached, nil
}

func (cd *Cache) Delete(ctx context.Context, key string) error {
	if cd.opt.LocalCache != nil {
		cd.opt.LocalCache.Del([]byte(key))
	}

	if cd.opt.Redis == nil {
		if cd.opt.LocalCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	_, err := cd.opt.Redis.Del(ctx, key).Result()
	return err
}

func (cd *Cache) localSet(key string, b []byte) {
	if cd.opt.LocalCacheTTL > 0 {
		pos := len(b)
		b = append(b, make([]byte, 4)...)
		encodeTime(b[pos:], time.Now())
	}

	cd.opt.LocalCache.Set([]byte(key), b)
}

func (cd *Cache) localGet(key string) ([]byte, bool) {
	b, ok := cd.opt.LocalCache.HasGet(nil, []byte(key))
	if !ok {
		return b, false
	}

	if len(b) == 0 || cd.opt.LocalCacheTTL == 0 {
		return b, true
	}
	if len(b) < timeLen {
		panic("not reached")
	}

	tm := decodeTime(b[len(b)-timeLen:])
	if time.Since(tm) > cd.opt.LocalCacheTTL {
		cd.opt.LocalCache.Del([]byte(key))
		return nil, false
	}

	return b[:len(b)-timeLen], true
}

func (cd *Cache) Marshal(value interface{}) ([]byte, error) {
	switch value := value.(type) {
	case nil:
		return nil, nil
	case []byte:
		return value, nil
	case string:
		return []byte(value), nil
	}

	return cd.opt.Marshaller.Marshal(value)
}

func (cd *Cache) Unmarshal(b []byte, value interface{}) error {
	if len(b) == 0 {
		return nil
	}

	switch value := value.(type) {
	case nil:
		return nil
	case *[]byte:
		clone := make([]byte, len(b))
		copy(clone, b)
		*value = clone
		return nil
	case *string:
		*value = string(b)
		return nil
	}

	return cd.opt.Marshaller.Unmarshal(b, value)
}

//------------------------------------------------------------------------------

type Stats struct {
	Hits   uint64
	Misses uint64
}

// Stats returns cache statistics.
func (cd *Cache) Stats() *Stats {
	if !cd.opt.StatsEnabled {
		return nil
	}
	return &Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
}

//------------------------------------------------------------------------------

var epoch = time.Date(2020, time.January, 01, 00, 0, 0, 0, time.UTC).Unix()

func encodeTime(b []byte, tm time.Time) {
	secs := tm.Unix() - epoch
	binary.LittleEndian.PutUint32(b, uint32(secs))
}

func decodeTime(b []byte) time.Time {
	secs := binary.LittleEndian.Uint32(b)
	return time.Unix(int64(secs)+epoch, 0)
}
