package server

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/google/uuid"
	"github.com/innovationb1ue/RedisGO/logger"
	"github.com/innovationb1ue/RedisGO/raftexample"
	"github.com/innovationb1ue/RedisGO/resp"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type Handler interface {
	Handle(context.Context, net.Conn)
}

type SimpleHandler struct {
	m *Manager
}

func NewSimpleHandler(m *Manager) Handler {
	return &SimpleHandler{m: m}
}

// Handle distributes all the client command to execute
func (handler *SimpleHandler) Handle(ctx context.Context, conn net.Conn) {
	// gracefully close the tcp connection to client
	defer func() {
		err := conn.Close()
		if err != nil {
			logger.Error(err)
		}
	}()

	// 1. 建立conn通道
	// create a goroutine that reads from the client and pump data into ch
	ch := resp.ParseStream(ctx, conn)
	// parsedRes is a complete command read from client
	for {
		select {
		// 2. 接收conn数据
		case parsedRes := <-ch:
			// handle errors
			if parsedRes.Err != nil {
				if parsedRes.Err == io.EOF {
					logger.Info("Close connection ", conn.RemoteAddr().String())
				} else {
					logger.Panic("Handle connection ", conn.RemoteAddr().String(), " panic: ", parsedRes.Err.Error())
				}
				return
			}
			// empty msg
			if parsedRes.Data == nil {
				logger.Error("empty parsedRes.Data from ", conn.RemoteAddr().String())
				continue
			}

			// 3. conn输入数据转化为array输入
			// handling array command
			arrayData, ok := parsedRes.Data.(*resp.ArrayData)
			if !ok {
				logger.Error("parsedRes.Data is not ArrayData from ", conn.RemoteAddr().String())
				continue
			}

			// 4. array输入转化为cmd
			// extract [][]bytes command
			cmd := arrayData.ToCommand()
			// run the string command when in standalone mode
			// also pass connection as an argument since the command may block and return continuous messages
			// 5. 执行cmd
			res := handler.m.ExecCommand(ctx, cmd, conn)

			// 6. 写入结果
			// return result
			if res != nil {
				_, err := conn.Write(res.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
			} else {
				// return error
				errData := resp.MakeErrorData("unknown error")
				_, err := conn.Write(errData.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

type ClusterHandler struct {
	m *Manager

	proposeC    chan<- *raftexample.RaftProposal
	confChangeC chan<- raftpb.ConfChangeI
	callback    map[string]chan resp.RedisData
	filter      *middleware
}

func NewClusterHandler(m *Manager, proposeC chan<- *raftexample.RaftProposal,
	confChangeC chan<- raftpb.ConfChangeI,
	callback map[string]chan resp.RedisData,
	filter *middleware) Handler {
	return &ClusterHandler{
		m:           m,
		proposeC:    proposeC,
		confChangeC: confChangeC,
		callback:    callback,
		filter:      filter,
	}
}

// HandleCluster handle the client commands from cli (tcp connection stream).
func (handler *ClusterHandler) Handle(ctx context.Context, conn net.Conn) {
	// gracefully close the tcp connection to client
	defer func() {
		err := conn.Close()
		if err != nil {
			logger.Error(err)
		}
	}()
	// create a goroutine that reads from the client and pump data into ch
	ch := resp.ParseStream(ctx, conn)

	// confChangeC pointer放入Context有利于维持接口的规范统一和简洁
	ctx = context.WithValue(ctx, "confChangeC", handler.confChangeC)
	// parsedRes is a complete command read from client
	for {
		select {
		case parsedRes := <-ch:
			// handle errors
			if parsedRes.Err != nil {
				if parsedRes.Err == io.EOF {
					logger.Info("Close connection ", conn.RemoteAddr().String())
				} else {
					logger.Panic("Handle connection ", conn.RemoteAddr().String(), " panic: ", parsedRes.Err.Error())
				}
				return
			}
			// empty msg
			if parsedRes.Data == nil {
				logger.Error("empty parsedRes.Data from ", conn.RemoteAddr().String())
				continue
			}
			// handling array command
			arrayData, ok := parsedRes.Data.(*resp.ArrayData)
			if !ok {
				logger.Error("parsedRes.Data is not ArrayData from ", conn.RemoteAddr().String())
				continue
			}
			// extract [][]bytes command
			cmd := arrayData.ToCommand()
			// get command passed through filters
			var err error
			cmd, err = handler.filter.Filter(cmd)
			if err != nil {
				logger.Error("filter error ", err)
				// return error
				errData := resp.MakeErrorData("command does not pass checks")
				_, err := conn.Write(errData.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
				continue
			}
			cmdStrings := arrayData.ToStringCommand()

			// confChangeC提交改变结果
			// confChange command
			// todo: temporary workaround for confChange propose
			// might treat the rconf command as a normal command and wait for master to accept it and return response
			if cmdStrings[0] == "rconf" {
				res := handler.m.ExecCommand(ctx, cmd, conn)
				_, err := conn.Write(res.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
				continue
			}

			// proposeC command to raft cluster
			cmdID := uuid.NewString()
			handler.callback[cmdID] = make(chan resp.RedisData)
			// todo: decide which command needs to be proposed to cluster
			// For example, query command does not need to be proposed.
			proposal := &raftexample.RaftProposal{
				Data: strings.Join(cmdStrings, " "),
				ID:   cmdID,
			}
			handler.proposeC <- proposal
			res := <-handler.callback[cmdID]
			delete(handler.callback, cmdID)
			// return result
			if res != nil {
				_, err := conn.Write(res.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
			} else {
				// return error
				errData := resp.MakeErrorData("unknown error")
				_, err := conn.Write(errData.ToBytes())
				if err != nil {
					logger.Error("write response to ", conn.RemoteAddr().String(), " error: ", err.Error())
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
