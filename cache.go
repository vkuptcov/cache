package cache

import (
	"errors"
	"fmt"
	"github.com/go-redis/cache/v7/internal/lrucache"
	"github.com/go-redis/cache/v7/internal/singleflight"
	"log"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v7"
)

var ErrCacheMiss = errors.New("cache: key is missing")
var errRedisLocalCacheNil = errors.New("cache: both Redis and LocalCache are nil")

type rediser interface {
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Get(key string) *redis.StringCmd
	Del(keys ...string) *redis.IntCmd
	Pipeline() redis.Pipeliner
}

type Item struct {
	Key    string
	Object interface{}

	// Func returns object to be cached.
	Func func() (interface{}, error)

	// Expiration is the cache expiration time.
	// Default expiration is 1 hour.
	Expiration time.Duration

	// doNotReturn is internal field which marks the item as non-returnable from cache
	// it can be used in case of BatchArgs.CreateObjectForMissedKey usage
	doNotReturn bool
}

type MGetArgs struct {
	// Keys to load
	Keys []string

	// A destination object which must be a key-value map
	Dst interface{}

	// Func returns a map of objects which corresponds to the provided cache keys
	ObjByCacheKeyLoader func(keysToLoad []string) (map[string]interface{}, error)

	// Expiration is the cache expiration time.
	// Default expiration is 1 hour.
	Expiration time.Duration
}

type BatchArgs struct {
	// Keys to load
	Keys []string

	// Dst is a slice of all loaded objects (both from cache and the BatchLoader)
	Dst interface{}

	// ItemToKey transforms a desired object to it's key
	ItemToKey func(interface{}) string

	// CollectMissedKey is used to modify args for the BatchLoader func
	CollectMissedKey func(key string)

	// CreateObjectForMissedKey returns an object in case it wasn't loaded via BatchLoader
	// it can be used to populate the cache with the missed data in order to avoid hitting database
	CreateObjectForMissedKey  func(key string) (objectToCache interface{}, returnInResult bool)

	// BatchLoader returns a slice of objects to be cached
	BatchLoader func() (interface{}, error)

	// Expiration is the cache expiration time.
	// Default expiration is 1 hour.
	Expiration time.Duration
}

func (item *Item) object() (interface{}, error) {
	if item.Object != nil {
		return item.Object, nil
	}
	if item.Func != nil {
		return item.Func()
	}
	return nil, nil
}

type Codec struct {
	Redis rediser

	defaultRedisExpiration time.Duration

	localCache *lrucache.Cache

	Marshal   func(interface{}) ([]byte, error)
	Unmarshal func([]byte, interface{}) error

	group singleflight.Group

	hits        uint64
	misses      uint64
	localHits   uint64
	localMisses uint64
}

// UseLocalCache causes Codec to cache items in local LRU cache.
func (cd *Codec) UseLocalCache(maxLen int, expiration time.Duration) {
	cd.localCache = lrucache.New(maxLen, expiration)
}

func (cd *Codec) SetDefaultRedisExpiration(expiration time.Duration) {
	cd.defaultRedisExpiration = expiration
	cd.ensureDefaultExp()
}

// Set caches the item.
func (cd *Codec) Set(items ...*Item) error {
	if len(items) == 1 {
		_, err := cd.setItem(items[0])
		return err
	} else if len(items) > 1 {
		return cd.mSetItems(items)
	}
	return nil
}

func (cd *Codec) setItem(item *Item) ([]byte, error) {
	object, err := item.object()
	if err != nil {
		return nil, err
	}

	b, err := cd.Marshal(object)
	if err != nil {
		log.Printf("cache: Marshal key=%q failed: %s", item.Key, err)
		return nil, err
	}

	if cd.localCache != nil {
		cd.localCache.Set(item.Key, b)
	}

	if cd.Redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return b, nil
	}

	err = cd.Redis.Set(item.Key, b, cd.exp(item.Expiration)).Err()
	if err != nil {
		log.Printf("cache: Set key=%q failed: %s", item.Key, err)
	}
	return b, err
}

// Exists reports whether object for the given key exists.
func (cd *Codec) Exists(key string) bool {
	return cd.Get(key, nil) == nil
}

// Get gets the object for the given key.
func (cd *Codec) Get(key string, object interface{}) error {
	return cd.get(key, object, false)
}

func (cd *Codec) get(key string, object interface{}, onlyLocalCache bool) error {
	b, err := cd.getBytes(key, onlyLocalCache)
	if err != nil {
		return err
	}

	if object == nil || len(b) == 0 {
		return nil
	}

	err = cd.Unmarshal(b, object)
	if err != nil {
		log.Printf("cache: key=%q Unmarshal(%T) failed: %s", key, object, err)
		return err
	}

	return nil
}

func (cd *Codec) MGet(dst interface{}, keys ...string) error {
	mapValue := reflect.ValueOf(dst)
	if mapValue.Kind() == reflect.Ptr {
		// get the value that the pointer mapValue points to.
		mapValue = mapValue.Elem()
	}
	if mapValue.Kind() != reflect.Map {
		return fmt.Errorf("dst must be a map instead of %v", mapValue.Type())
	}
	mapType := mapValue.Type()

	// get the type of the key.
	keyType := mapType.Key()
	if keyType.Kind() != reflect.String {
		return fmt.Errorf("dst key type must be a string, %v given", keyType.Kind())
	}

	elementType := mapType.Elem()
	// non-pointer values not supported yet
	if elementType.Kind() != reflect.Ptr {
		return fmt.Errorf("dst value type must be a pointer, %v given", elementType.Kind())
	}
	// get the value that the pointer elementType points to.
	elementType = elementType.Elem()

	// allocate a new map, if mapValue is nil.
	// @todo fix "reflect.Value.Set using unaddressable value"
	if mapValue.IsNil() {
		mapValue.Set(reflect.MakeMap(mapType))
	}

	res, err := cd.mGetBytes(keys)
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
		mapValue.SetMapIndex(key, reflect.ValueOf(dstEl))
	}

	return nil
}

func (cd *Codec) BatchLoadAndCache(batchArgs *BatchArgs) error {
	dstSl := reflect.ValueOf(batchArgs.Dst)
	if dstSl.Kind()  == reflect.Ptr {
		dstSl = dstSl.Elem()
	}
	if dstSl.Kind() != reflect.Slice {
		return fmt.Errorf("slice expected as a destination, %s received", dstSl.Kind())
	}
	m := reflect.MakeMap( reflect.MapOf(reflect.TypeOf(""), dstSl.Type().Elem())).Interface()
	mArgs := &MGetArgs{
		Keys: batchArgs.Keys,
		Dst:  m,
		ObjByCacheKeyLoader: func(keysToLoad []string) (map[string]interface{}, error) {
			for _, k := range keysToLoad {
				batchArgs.CollectMissedKey(k)
			}
			loadedItems, err := batchArgs.BatchLoader()
			if err != nil {
				return nil, err
			}
			li := reflect.ValueOf(loadedItems)
			if li.Kind() == reflect.Ptr {
				li = li.Elem()
			}
			var result map[string]interface{}
			switch li.Kind() {
			case reflect.Slice:
				result = make(map[string]interface{}, li.Len())
				for i := 0; i < li.Len(); i++ {
					elem := li.Index(i).Interface()
					result[batchArgs.ItemToKey(elem)] = elem
				}
			default:
				return nil, fmt.Errorf("slice expected from the loader function, %s received", li.Kind())
			}
			if len(keysToLoad) != len(result) && batchArgs.CreateObjectForMissedKey != nil {
				for _, k := range keysToLoad {
					if _, exists := result[k]; !exists {
						objToCache, returnInResult := batchArgs.CreateObjectForMissedKey(k)
						if returnInResult {
							result[k] = objToCache
						} else {
							result[k] = &Item{
								Key:         k,
								Object:      objToCache,
								Expiration:  batchArgs.Expiration,
								doNotReturn: true,
							}
						}
					}
				}
			}
			return result, nil
		},
		Expiration: batchArgs.Expiration,
	}
	err :=  cd.MGetAndCache(mArgs)
	if err != nil {
		return err
	}
	reflectedMap := reflect.ValueOf(mArgs.Dst)
	for _, k := range reflectedMap.MapKeys() {
		v := reflectedMap.MapIndex(k)
		dstSl = reflect.Append(dstSl, v)
	}
	batchArgs.Dst = dstSl.Interface()
	return nil
}

func (cd *Codec) MGetAndCache(mItem *MGetArgs) error {
	err := cd.MGet(mItem.Dst, mItem.Keys...)
	if err != nil {
		return err
	}
	m := reflect.ValueOf(mItem.Dst)
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	// map type is checked in the MGet function
	if m.Len() != len(mItem.Keys) {
		absentKeys := make([]string, len(mItem.Keys)-m.Len())
		idx := 0
		for _, k := range mItem.Keys {
			mapVal := m.MapIndex(reflect.ValueOf(k))
			if !mapVal.IsValid() {
				absentKeys[idx] = k
				idx++
			}
		}
		loadedData, loaderErr := mItem.ObjByCacheKeyLoader(absentKeys)
		if loaderErr != nil {
			return loaderErr
		}

		items := make([]*Item, len(loadedData))
		i := 0
		for key, d := range loadedData {
			var item *Item
			var obj interface{}
			if it, ok := d.(*Item); ok {
				item = it
				obj = it.Object
			} else {
				item = &Item{
					Key:        key,
					Object:     d,
					Expiration: mItem.Expiration,
				}
				obj = d
			}
			if !item.doNotReturn {
				m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(obj))
			}
			items[i] = item
			i++
		}
		return cd.Set(items...)
	}
	return nil
}

func (cd *Codec) mSetItems(items []*Item) error {
	var pipeline redis.Pipeliner
	if cd.Redis != nil {
		pipeline = cd.Redis.Pipeline()
	}
	for _, item := range items {
		key := item.Key
		bytes, e := cd.Marshal(item.Object)
		if e != nil {
			return e
		}
		if cd.localCache != nil {
			cd.localCache.Set(key, bytes)
		}
		if pipeline != nil {
			pipeline.Set(key, bytes, cd.exp(item.Expiration))
		}
	}
	if pipeline != nil {
		_, err := pipeline.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

// mGetBytes actually returns [][]bytes in an order which corresponds to the provided keys
// an interface{} is used to not allocate intermediate structures
func (cd *Codec) mGetBytes(keys []string) ([]interface{}, error) {
	collectedData := make([]interface{}, len(keys))
	recordsMissedInLocalCache := len(keys)
	if cd.localCache != nil {
		for idx, k := range keys {
			var err error
			var d []byte
			d, err = cd.getBytes(k, true)
			if err == nil {
				collectedData[idx] = d
				recordsMissedInLocalCache--
			}
		}
	}

	if cd.Redis != nil && recordsMissedInLocalCache > 0 {
		pipeline := cd.Redis.Pipeline()
		for idx, b := range collectedData {
			if b == nil {
				// the pipeline result is stored here to be able not to store indexes for non-local keys
				collectedData[idx] = pipeline.Get(keys[idx])
			}
		}
		_, err := pipeline.Exec()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		hits := 0
		for idx, content := range collectedData {
			if redisResp, ok := content.(*redis.StringCmd); ok {
				data, err := redisResp.Result()
				if err == redis.Nil {
					collectedData[idx] = nil
					continue
				}
				if err != nil {
					return nil, err
				}
				collectedData[idx] = []byte(data)
				hits++
			}
		}
		misses := recordsMissedInLocalCache - hits
		atomic.AddUint64(&cd.hits, uint64(hits))
		atomic.AddUint64(&cd.misses, uint64(misses))
	}

	return collectedData, nil
}

func (cd *Codec) getBytes(key string, onlyLocalCache bool) ([]byte, error) {
	if cd.localCache != nil {
		b, ok := cd.localCache.Get(key)
		if ok {
			atomic.AddUint64(&cd.localHits, 1)
			return b, nil
		}
		atomic.AddUint64(&cd.localMisses, 1)
	}

	if onlyLocalCache {
		return nil, ErrCacheMiss
	}
	if cd.Redis == nil {
		if cd.localCache == nil {
			return nil, errRedisLocalCacheNil
		}
		return nil, ErrCacheMiss
	}

	b, err := cd.Redis.Get(key).Bytes()
	if err != nil {
		atomic.AddUint64(&cd.misses, 1)
		if err == redis.Nil {
			return nil, ErrCacheMiss
		}
		log.Printf("cache: Get key=%q failed: %s", key, err)
		return nil, err
	}
	atomic.AddUint64(&cd.hits, 1)

	if cd.localCache != nil {
		cd.localCache.Set(key, b)
	}
	return b, nil
}

// Once gets the item.Object for the given item.Key from the cache or
// executes, caches, and returns the results of the given item.Func,
// making sure that only one execution is in-flight for a given item.Key
// at a time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (cd *Codec) Once(item *Item) error {
	b, cached, err := cd.getSetItemBytesOnce(item)
	if err != nil {
		return err
	}

	if item.Object == nil || len(b) == 0 {
		return nil
	}

	err = cd.Unmarshal(b, item.Object)
	if err != nil {
		log.Printf("cache: key=%q Unmarshal(%T) failed: %s", item.Key, item.Object, err)
		if cached {
			_ = cd.Delete(item.Key)
			return cd.Once(item)
		} else {
			return err
		}
	}

	return nil
}

func (cd *Codec) getSetItemBytesOnce(item *Item) (b []byte, cached bool, err error) {
	if cd.localCache != nil {
		b, err := cd.getItemBytesFast(item)
		if err == nil {
			return b, true, nil
		}
	}

	obj, err := cd.group.Do(item.Key, func() (interface{}, error) {
		b, err := cd.getItemBytes(item)
		if err == nil {
			cached = true
			return b, nil
		}

		obj, err := item.Func()
		if err != nil {
			return nil, err
		}

		b, err = cd.setItem(&Item{
			Key:        item.Key,
			Object:     obj,
			Expiration: item.Expiration,
		})
		if b != nil {
			// Ignore error if we have the result.
			return b, nil
		}
		return nil, err
	})
	if err != nil {
		return nil, false, err
	}
	return obj.([]byte), cached, nil
}

func (cd *Codec) getItemBytes(item *Item) ([]byte, error) {
	return cd.getBytes(item.Key, false)
}

func (cd *Codec) getItemBytesFast(item *Item) ([]byte, error) {
	return cd.getBytes(item.Key, true)
}

func (cd *Codec) exp(itemExp time.Duration) time.Duration {
	if itemExp < 0 {
		return 0
	}
	if itemExp < time.Second {
		cd.ensureDefaultExp()
		return cd.defaultRedisExpiration
	}
	return itemExp
}

func (cd *Codec) ensureDefaultExp() {
	if cd.defaultRedisExpiration < time.Second {
		cd.defaultRedisExpiration = time.Hour
	}
}

func (cd *Codec) Delete(keys ...string) error {
	if cd.localCache != nil {
		for _, key := range keys {
			cd.localCache.Delete(key)
		}

	}

	if cd.Redis == nil {
		if cd.localCache == nil {
			return errRedisLocalCacheNil
		}
		return nil
	}

	pipeline := cd.Redis.Pipeline()

	for _, key := range keys {
		pipeline.Del(key)
	}
	_, err := pipeline.Exec()
	return err
}

type Stats struct {
	Hits        uint64
	Misses      uint64
	LocalHits   uint64
	LocalMisses uint64
}

// Stats returns cache statistics.
func (cd *Codec) Stats() *Stats {
	stats := Stats{
		Hits:   atomic.LoadUint64(&cd.hits),
		Misses: atomic.LoadUint64(&cd.misses),
	}
	if cd.localCache != nil {
		stats.LocalHits = atomic.LoadUint64(&cd.localHits)
		stats.LocalMisses = atomic.LoadUint64(&cd.localMisses)
	}
	return &stats
}
