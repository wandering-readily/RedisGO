package server

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/innovationb1ue/RedisGO/config"
	"github.com/innovationb1ue/RedisGO/memdb"
	"github.com/innovationb1ue/RedisGO/resp"
)

// Manager handles all client requests to the server
// It holds multiple MemDb instances
type Manager struct {
	CurrentDB *memdb.MemDb
	DBs       []*memdb.MemDb
}

type MemStorageStats struct {
	initialState, firstIndex, lastIndex, entries, term, snapshot int
}

// NewManager creates a default Manager
func NewManager(cfg *config.Config) *Manager {
	DBs := make([]*memdb.MemDb, cfg.Databases)
	for i := 0; i < cfg.Databases; i++ {
		DBs[i] = memdb.NewMemDb()
	}
	return &Manager{
		CurrentDB: DBs[0],
		DBs:       DBs,
	}
}

func (m *Manager) ExecCommand(ctx context.Context, cmd [][]byte, conn net.Conn) resp.RedisData {
	if len(cmd) == 0 {
		return nil
	}
	var res resp.RedisData
	cmdName := strings.ToLower(string(cmd[0]))
	// 选择数据库
	// global commands
	switch cmdName {
	case "select":
		return m.Select(cmd)
	}
	// get the command from hash table and execute it.
	command, ok := memdb.CmdTable[cmdName]
	if !ok {
		res = resp.MakeErrorData("ERR unknown command ", cmdName)
	} else {
		res = command.Executor(ctx, m.CurrentDB, cmd, conn)
	}
	return res
}

func (m *Manager) ExecStrCommand(ctx context.Context, cmdStr string, conn net.Conn) resp.RedisData {
	cmd := strings.Split(cmdStr, " ")
	if len(cmd) == 0 {
		return nil
	}
	var res resp.RedisData
	cmdName := strings.ToLower(cmd[0])
	// global commands
	byteCmd := make([][]byte, 0)
	for _, s := range cmd {
		byteCmd = append(byteCmd, []byte(s))
	}
	switch cmdName {
	case "select":
		return m.Select(byteCmd)
	}
	// get the command from hash table and execute it.
	command, ok := memdb.CmdTable[cmdName]
	if !ok {
		res = resp.MakeErrorData("ERR unknown command ", cmdName)
	} else {
		res = command.Executor(ctx, m.CurrentDB, byteCmd, conn)
	}
	return res
}

func (m *Manager) Select(cmd [][]byte) resp.RedisData {
	if len(cmd) != 2 {
		return resp.MakeWrongNumberArgs("select")
	}
	dbIdx, err := strconv.Atoi(string(cmd[1]))
	if err != nil {
		return resp.MakeErrorData("ERR value is not an integer or out of range")
	}
	if dbIdx >= len(m.DBs) || dbIdx < 0 {
		return resp.MakeErrorData(fmt.Sprintf("ERR DB index is out of range with maximum %d", len(m.DBs)))
	}
	m.CurrentDB = m.DBs[dbIdx]
	return resp.MakeStringData("OK")
}
