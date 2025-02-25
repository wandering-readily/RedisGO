package memdb

import (
	"sync"
	"sync/atomic"

	"github.com/innovationb1ue/RedisGO/util"
)

const MaxConSize = int(1<<31 - 1)

// ConcurrentMap manage a table slice with multiple hashmap shards to avoid lock bottleneck.
// It is threads safe by using rwLock.
// it supports maximum table size = MaxConSize
type ConcurrentMap struct {
	table []*shard
	size  int   // table size (fixed)
	count int64 // total number of keys
}

// shard is the object that represents a k:v pair in redis
type shard struct {
	item map[string]any
	rwMu *sync.RWMutex
}

// NewConcurrentMap create a new ConcurrentMap with given size. If size <=0, it will be set to MaxConSize.
func NewConcurrentMap(size int) *ConcurrentMap {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	m := &ConcurrentMap{
		table: make([]*shard, size),
		size:  size,
		count: 0,
	}
	// fill shards
	for i := 0; i < size; i++ {
		m.table[i] = &shard{item: make(map[string]any), rwMu: &sync.RWMutex{}}
	}
	return m
}

func (m *ConcurrentMap) getKeyPos(key string) int {
	return util.HashKey(key) % m.size
}

func (m *ConcurrentMap) getShard(key string) *shard {
	return m.table[m.getKeyPos(key)]
}

// Set sets a key to the value.
func (m *ConcurrentMap) Set(key string, value any) int {
	added := 0
	shard := m.getShard(key)
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.item[key]; !ok {
		atomic.AddInt64(&m.count, 1)
		added = 1
	}
	shard.item[key] = value
	return added
}

func (m *ConcurrentMap) SetIfExist(key string, value any) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.item[key]; ok {
		shard.item[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) SetIfNotExist(key string, value any) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.item[key]; !ok {
		atomic.AddInt64(&m.count, 1)
		shard.item[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) Get(key string) (any, bool) {
	pos := m.getKeyPos(key)
	// lock shard for reading.
	// this ensures no write will be synchronized before this read finished.
	shard := m.table[pos]
	shard.rwMu.RLock()
	defer shard.rwMu.RUnlock()

	value, ok := shard.item[key]
	return value, ok
}

func (m *ConcurrentMap) Delete(key string) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.item[key]; ok {
		delete(shard.item, key)
		atomic.AddInt64(&m.count, -1)
		return 1
	} else {
		return 0
	}
}

func (m *ConcurrentMap) Len() int64 {
	return atomic.LoadInt64(&m.count)
}

func (m *ConcurrentMap) Clear() {
	*m = *NewConcurrentMap(m.size)
}

// 是否应该使用全局锁
// Keys return all stored keys in the concurrent map
func (m *ConcurrentMap) Keys() []string {
	keys := make([]string, 0)
	i := 0
	for _, shard := range m.table {
		shard.rwMu.RLock()
		keys = append(keys, make([]string, len(shard.item))...)
		for key := range shard.item {
			keys[i] = key
			i++
		}
		shard.rwMu.RUnlock()
	}
	return keys
}

func (m *ConcurrentMap) KeyVals() map[string]any {
	res := make(map[string]any)
	i := 0
	for _, shard := range m.table {
		shard.rwMu.RLock()
		for k, v := range shard.item {
			res[k] = v
			i++
		}
		shard.rwMu.RUnlock()
	}
	return res
}
