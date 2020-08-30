package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
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
}

func New(opt *Options) *Cache {
	opt.init()
	return &Cache{
		opt: opt,
	}
}

// Set sets multiple elements
// @todo unify with Set
func (cd *Cache) Set(ctx context.Context, items ...*Item) (err error) {
	r := cd.opt.Redis
	var pipeliner redis.Pipeliner
	if len(items) > 1 && r != nil {
		pipeliner = cd.opt.Redis.Pipeline()
		r = pipeliner
	}
	for _, item := range items {
		_, _, err = cd.set(ctx, r, item)
		if err != nil {
			return err
		}
	}
	if pipeliner != nil {
		_, err = pipeliner.Exec(ctx)
	}
	return err
}

func (cd *Cache) set(ctx context.Context, redis rediser, item *Item) ([]byte, bool, error) {
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
		return b, true, redis.SetXX(ctx, item.Key, b, cd.redisTTL(item)).Err()
	}

	if item.IfNotExists {
		return b, true, redis.SetNX(ctx, item.Key, b, cd.redisTTL(item)).Err()
	}

	return b, true, redis.Set(ctx, item.Key, b, cd.redisTTL(item)).Err()
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

func (cd *Cache) MGet(ctx context.Context, dst interface{}, keys ...string) error {
	reflectValue := reflect.ValueOf(dst)
	if reflectValue.Kind() == reflect.Ptr {
		// get the dst that the pointer reflectValue points to.
		reflectValue = reflectValue.Elem()
	}

	var elementType reflect.Type
	switch reflectValue.Kind() {
	case reflect.Map:
		mapType := reflectValue.Type()
		// get the type of the key.
		keyType := mapType.Key()
		if keyType.Kind() != reflect.String {
			return fmt.Errorf("dst key type must be a string, %v given", keyType.Kind())
		}
		// allocate a new map, if reflectValue is nil.
		// @todo allocate a map with the right size
		if reflectValue.IsNil() {
			reflectValue.Set(reflect.MakeMap(mapType))
		}
		elementType = mapType.Elem()
	case reflect.Slice:
		sliceType := reflectValue.Type()
		// allocate a new map, if reflectValue is nil.
		// @todo allocate a slice with the right size
		if reflectValue.IsNil() {
			reflectValue.Set(reflect.MakeSlice(sliceType, 0, len(keys)))
		}
		elementType = sliceType.Elem()
	default:
		return fmt.Errorf("dst must be a map or a slice instead of %v", reflectValue.Type())
	}

	nonPointerValue := true
	if elementType.Kind() == reflect.Ptr {
		// get the dst that the pointer elementType points to.
		elementType = elementType.Elem()
		nonPointerValue = false
	}

	res, err := cd.mGetBytes(ctx, keys)
	if err != nil {
		return err
	}

	for idx, data := range res {
		bytes, ok := data.([]byte)
		if !ok || bytes == nil {
			continue
		}
		elementValue := reflect.New(elementType)
		dstEl := elementValue.Interface()

		err := cd.Unmarshal(bytes, dstEl)
		if err != nil {
			return err
		}
		key := reflect.ValueOf(keys[idx])
		val := reflect.ValueOf(dstEl)
		if nonPointerValue {
			val = val.Elem()
		}
		switch reflectValue.Kind() {
		case reflect.Map:
			reflectValue.SetMapIndex(key, val)
		case reflect.Slice:
			reflectValue = reflect.Append(reflectValue, val)
		}
	}
	return nil
}

func (cd *Cache) mGetBytes(ctx context.Context, keys []string) ([]interface{}, error) {
	collectedData := make([]interface{}, len(keys))
	recordsMissedInLocalCache := len(keys)
	if cd.opt.LocalCache != nil {
		for idx, k := range keys {
			if d, exists := cd.localGet(k); exists {
				collectedData[idx] = d
				recordsMissedInLocalCache--
			}
		}
	}

	if cd.opt.Redis != nil && recordsMissedInLocalCache > 0 {
		pipeliner := cd.opt.Redis.Pipeline()
		defer pipeliner.Close()
		for idx, b := range collectedData {
			if b == nil {
				// the pipeliner result is stored here to be able not to store indexes for non-local keys
				collectedData[idx] = pipeliner.Get(ctx, keys[idx])
			}
		}
		_, pipelinerErr := pipeliner.Exec(ctx)
		if pipelinerErr != nil && pipelinerErr != redis.Nil {
			return nil, pipelinerErr
		}
		for idx, content := range collectedData {
			if redisResp, ok := content.(*redis.StringCmd); ok {
				data, respErr := redisResp.Result()
				if respErr == redis.Nil {
					collectedData[idx] = nil
					continue
				}
				if respErr != nil {
					return nil, respErr
				}
				collectedData[idx] = []byte(data)
			}
		}
	}

	return collectedData, nil
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
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		return nil, err
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
func (cd *Cache) Once(ctx context.Context, item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(ctx, item)
	if err != nil {
		return err
	}

	if item.Value == nil || len(b) == 0 {
		return nil
	}

	if err := cd.Unmarshal(b, item.Value); err != nil {
		if cached {
			_ = cd.Delete(ctx, item.Key)
			return cd.Once(ctx, item)
		}
		return err
	}

	return nil
}

func (cd *Cache) getSetItemBytesOnce(ctx context.Context, item *Item) (b []byte, cached bool, err error) {
	if cd.opt.LocalCache != nil {
		b, ok := cd.localGet(item.Key)
		if ok {
			return b, true, nil
		}
	}

	v, err, _ := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getBytes(ctx, item.Key, item.SkipLocalCache)
		if err == nil {
			cached = true
			return b, nil
		}

		b, ok, err := cd.set(ctx, cd.opt.Redis, item)
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
