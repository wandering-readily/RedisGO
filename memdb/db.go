package memdb

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"time"

	"github.com/innovationb1ue/RedisGO/raftexample"

	"github.com/innovationb1ue/RedisGO/config"
	"github.com/innovationb1ue/RedisGO/logger"
	"github.com/innovationb1ue/RedisGO/resp"
)

// MemDb is the memory cache database
// All key:value pairs are stored in db
// All ttl keys are stored in ttlKeys
// locks is used to lock a key for db to ensure some atomic operations
// SubChans are an independent concurrent map of channel shards used in PUB/SUB commands
type MemDb struct {
	db       *ConcurrentMap	// ConcurrentMap锁 锁住取改key value过程
	ttlKeys  *ConcurrentMap
	locks    *Locks // 锁住 value any操作过程
	SubChans *ChanMap
	Raft     *raftexample.RaftNode
}

func NewMemDb() *MemDb {
	return &MemDb{
		db:       NewConcurrentMap(config.Configures.ShardNum),
		ttlKeys:  NewConcurrentMap(config.Configures.ShardNum),
		locks:    NewLocks(config.Configures.ShardNum * 2),
		SubChans: NewChanMap(config.Configures.ShardNum),
		Raft:     nil,
	}
}

type TTLInfo struct {
	value  int64
	cancel chan struct{}
}

func (m *MemDb) ExecCommand(ctx context.Context, cmd [][]byte, conn net.Conn) resp.RedisData {
	if len(cmd) == 0 {
		return nil
	}
	var res resp.RedisData
	cmdName := strings.ToLower(string(cmd[0]))
	// get the command from hash table and execute it.
	command, ok := CmdTable[cmdName]
	if !ok {
		res = resp.MakeErrorData("ERR unknown command ", cmdName)
	} else {
		res = command.Executor(ctx, m, cmd, conn)
	}
	return res
}

// CheckTTL check ttl keys and delete expired keys
// return false if key is expired, else true.
// Attention: Don't lock this function because it has called locks.Lock(key) for atomic deleting expired key.
// Otherwise, it will cause a deadlock.
func (m *MemDb) CheckTTL(key string) bool {
	ttl, ok := m.ttlKeys.Get(key)
	if !ok {
		return true
	}
	ttlTime := ttl.(*TTLInfo)
	now := time.Now().Unix()
	if ttlTime.value > now {
		return true
	}
	// if it should expire
	m.locks.Lock(key)
	defer m.locks.UnLock(key)
	m.db.Delete(key)
	m.ttlKeys.Delete(key)
	return false
}

// SetTTL set ttl for key
// return bool to check if ttl set success
// return int to check if the key is a new ttl key
// value: seconds at expire
func (m *MemDb) SetTTL(key string, value int64) int {
	if _, ok := m.db.Get(key); !ok {
		logger.Debug("SetTTL: key not exist")
		return 0
	}
	cancel := make(chan struct{})
	// remove old TTL task if exist
	ttlInfoTmp, ok := m.ttlKeys.Get(key)
	if ok {
		ttlInfo := ttlInfoTmp.(*TTLInfo)
		close(ttlInfo.cancel)
		m.ttlKeys.Delete(key)
	}
	// save TTL
	m.ttlKeys.Set(key, &TTLInfo{
		value:  value,
		cancel: cancel,
	})
	// start TTL check timed task
	go func() {
		select {
		// this chan fires after the ttl expire
		case <-time.After(time.Duration(value-time.Now().Unix()) * time.Second):
			// CheckTTL locks itself
			m.CheckTTL(key)
			log.Println("TLL fires")
		case <-cancel:
			log.Println("TTL canceled")
			return
		}
	}()
	return 1
}

func (m *MemDb) DelTTL(key string) int {
	// check ttl exist
	ttlTmp, ok := m.ttlKeys.Get(key)
	if !ok {
		return 0
	}
	// cancel timed TTL task
	ttl := ttlTmp.(*TTLInfo)
	close(ttl.cancel)
	// delete TTL key from cMap
	return m.ttlKeys.Delete(key)
}

func (m *MemDb) GetSnapshot() ([]byte, error) {
	// todo: change the snapshot format to rdb|aof
	return json.Marshal(m.db.KeyVals())
}
