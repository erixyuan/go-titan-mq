package core

import (
	"github.com/erixyuan/go-titan-mq/protocol"
	"net"
	"time"
)

// 定义客户端的结构
type Client struct {
	Conn          net.Conn
	ClientID      string    // 客户端ID， uuid 生成，连接的时候，返回给client
	LastHeartbeat time.Time // 上次执行命令的时候
	Status        int       // 在线状态， 1 在线， 0下线（何时标记为下线？当先客户端发送消息的时候，返回EOF）
	Queue         chan protocol.Message
}
