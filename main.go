package main

import (
	"fmt"
	"os"

	"github.com/innovationb1ue/RedisGO/config"
	"github.com/innovationb1ue/RedisGO/logger"
	"github.com/innovationb1ue/RedisGO/memdb"
	"github.com/innovationb1ue/RedisGO/server"
)

func init() {
	// Register commands
	memdb.RegisterKeyCommands()
	memdb.RegisterStringCommands()
	memdb.RegisterListCommands()
	memdb.RegisterSetCommands()
	memdb.RegisterHashCommands()
	memdb.RegisterPubSubCommands()
	memdb.RegisterSortedSetCommands()
	memdb.RegisterStreamCommands()
}

func main() {
	// setup config
	cfg, err := config.Setup()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// setup logger
	err = logger.SetUp(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if cfg.IsCluster {
		err = server.Start(cfg)
		if err != nil {
			os.Exit(1)
		}
	}
	// setup tcp server
	err = server.Start(cfg)
	if err != nil {
		os.Exit(1)
	}
}
