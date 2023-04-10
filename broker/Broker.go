package broker

import (
	"context"
	"errors"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/erixyuan/go-titan-mq/tools"
	"github.com/golang/protobuf/proto"
	"github.com/julienschmidt/httprouter"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

/**
subscribe 命令表示客户端订阅某个主题，这里将连接 conn 记录在该主题对应的订阅者集合中。
unsubscribe 命令表示客户端取消订阅某个主题，这里将连接 conn 从该主题对应的订阅者集合中删除。
publish 命令表示客户端发布一条消息，这里会遍历该主题对应的订阅者集合，将消息发送给所有的订阅者。

在处理 publish 命令时，需要注意如果向某个订阅者发送消息失败了，需要将该订阅者从该主题对应的订阅者集合中删除。

*/

var Log = log.New()
var LogLevel = log.InfoLevel

type Broker struct {
	clients            map[net.Conn]*Client // 方便从conn找到clientId
	topics             map[string]*Topic
	topicsLock         sync.Mutex
	consumerGroupLock  sync.Mutex
	consumerClientLock sync.Mutex
	commitLogMutex     sync.Mutex
	CommitLog          *CommitLog
	topicsFile         *os.File
	topicRouteManager  *TopicRouteManager
	reBalanceMap       map[string]bool // string = topic_groupName
}

func NewBroker() *Broker {
	broker := Broker{
		clients:            make(map[net.Conn]*Client),
		topics:             make(map[string]*Topic),
		topicsLock:         sync.Mutex{},
		consumerGroupLock:  sync.Mutex{},
		consumerClientLock: sync.Mutex{},
		commitLogMutex:     sync.Mutex{},
		reBalanceMap:       make(map[string]bool),
	}
	return &broker
}

func (b *Broker) Start(port int) error {
	Log.Infof("Broker 开始初始化................................")
	b.init()
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer tcpListener.Close()
	// 开启tcp的监听
	Log.Infof("Broker 开始初始化监听端口 port %d\n", port)
	go b.tcpListenerHandler(tcpListener)

	// 监听本地的HTTP连接
	httpListener, _ := net.Listen("tcp", ":9090")
	defer httpListener.Close()

	Log.Infof("开始创建http服务")
	// 创建HTTP服务器
	router := httprouter.New()
	handler := HttpHandler{
		broker: b,
	}
	router.GET("/", handler.Index)
	router.POST("/topic/add", handler.AddTopic)
	router.POST("/consumerGroup/add", handler.AddConsumerGroup)
	router.POST("/topic/db", handler.FetchTopicDb)
	router.GET("/topic/data", handler.FetchTopicData)
	router.GET("/topic/table", handler.FetchTopicTable)
	router.POST("/topic/list", handler.FetchTopicList)
	router.GET("/produce/message", handler.ProduceMessage)
	router.POST("/message/list", handler.FetchMessageList)

	corsHandler := handler.setCorsHeader(router)

	// 启动两个监听器
	err = http.ListenAndServe(":9091", corsHandler)
	if err != nil {
		Log.Fatal(err)
	}
	select {}
	return nil
}

func (b *Broker) tcpListenerHandler(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			Log.Errorf("监听客户端异常:%+v", err)
			continue
		}
		Log.Infof("新客户端连接成功:%s", conn.RemoteAddr())

		go b.ListeningClient(conn)
		go b.CheckHeartbeatHandler()
	}
}

func (b *Broker) ListeningClient(conn net.Conn) {
	for {
		buf := make([]byte, 1024*4)
		n, err := conn.Read(buf)
		if err != nil {
			Log.Errorf("读取客户端数据异常: %v\n", err)
			if err == io.EOF {
				Log.Infof("客户端连接断开：%s | %s，剔除client", conn.RemoteAddr(), b.clients[conn])
				b.RemoveClient(b.clients[conn].ClientID)
			}
			return
		}
		remotingCommand := &protocol.RemotingCommand{}
		err = proto.Unmarshal(buf[:n], remotingCommand)
		if err != nil {
			Log.Errorf("收到请求体，内容异常: error +%v", err)
			continue
		}
		if remotingCommand.Type == protocol.RemotingCommandType_RequestCommand {
			Log.Debugf("收到请求：%+v", *remotingCommand.Header)
			// 如果是请求
			switch remotingCommand.Header.Opaque {
			case protocol.OpaqueType_Heartbeat:
				Log.Debugf("收到心跳请求 %s", remotingCommand.RequestId)
				go b.HeartbeatHandler(remotingCommand.Body, conn, remotingCommand.RequestId)
			case protocol.OpaqueType_SyncTopicRouteInfo:
				Log.Debugf("收到同步路由信息请求 %s", remotingCommand.RequestId)
				// 同步路由信息
				go b.SyncTopicRouteInfoHandler(remotingCommand.Body, conn, remotingCommand.RequestId)
			case protocol.OpaqueType_Subscription:
				// 订阅消息
				go b.SubscribeHandler(remotingCommand.Body, conn)
			case protocol.OpaqueType_Unsubscription:
				//
			case protocol.OpaqueType_Publish:
			case protocol.OpaqueType_PullMessage:
				go b.PullMessageHandler(remotingCommand.Body, conn, remotingCommand.RequestId)
			case protocol.OpaqueType_SyncConsumeOffset:
				go b.SyncConsumeOffsetHandler(remotingCommand.Body, conn, remotingCommand.RequestId)

			}
		} else if remotingCommand.Type == protocol.RemotingCommandType_ResponseCommand {
			// 如果是响应，例如消息确认
			Log.Debugf("收到响应：%+v", remotingCommand.Header)
		}
	}
}

/**
Consumer是在向Broker订阅消息时，将ClientId信息发送给Broker的。
具体而言，当Consumer创建实例后，会向Name Server查询Broker的地址，
并建立与Broker的连接。然后，Consumer会发送SubscribeRequest请求体，
向Broker订阅消息。SubscribeRequest请求体中包含了Consumer Group、Topic、订阅表达式等信息，
也包括了Consumer的ClientId信息。Broker在接收到SubscribeRequest请求体后，会解析其中的字段，
并将Consumer的ClientId信息保存到Broker的订阅关系表中。这样，当有新的消息到达Broker时，Broker会根据订阅关系表中的信息，向相应的Consumer发送消息，
并在消息头中携带Consumer的ClientId信息。因此，Consumer的ClientId信息是在向Broker订阅消息的阶段传递给Broker的。
*/
func (b *Broker) SubscribeHandler(body []byte, conn net.Conn) {
	req := protocol.SubscriptionRequestData{}
	if err := proto.Unmarshal(body, &req); err != nil {
		b.Return(conn, protocol.OpaqueType_Subscription, nil, ErrRequest)
		return
	}
	Log.Infof("收到订阅请求，数据为：%+v", req)
	topicName := req.Topic
	clientId := req.ClientId
	consumerGroupName := req.ConsumerGroup
	// 检查topic是否存在
	if topic, ok := b.topics[topicName]; !ok {
		b.Return(conn, protocol.OpaqueType_Subscription, nil, ErrMessageNotYet)
		return
	} else {
		// 检查消费组是否存在
		if consumerGroup, ok := topic.consumerGroups[consumerGroupName]; !ok {
			Log.Infof("SubscribeHandler error: 消费组[%s]不存在", consumerGroupName)
			b.Return(conn, protocol.OpaqueType_Subscription, nil, ErrConsumerGroupNotExist)
			return
		} else {
			if _, ok := consumerGroup.Clients[clientId]; ok {
				// 已经存在了就返回订阅异常
				Log.Infof("SubscribeHandler error: 消费组中的ClientId已经存在")
				b.Return(conn, protocol.OpaqueType_Subscription, nil, ErrClientExist)
				return
			} else {
				// 注册消费者
				if client, err := b.topicRouteManager.RegisterConsumer(topicName, consumerGroupName, clientId, conn); err != nil {
					Log.Infof("SubscribeHandler error: %+v", err)
				} else {
					// 登记入当前的客户端表，方便查询
					Log.Infof("SubscribeHandler 登记clientId %s->%s", conn.RemoteAddr(), clientId)
					b.clients[conn] = client
					b.reBalance(topicName, consumerGroupName)
				}
			}
		}
	}

	queues := b.topicRouteManager.FindQueues(req.Topic, req.ConsumerGroup, req.ClientId)
	// 返回
	responseData := protocol.SubscriptionResponseData{
		Topic:           topicName,
		ClientId:        clientId,
		ConsumerGroup:   consumerGroupName,
		ConsumeProgress: queues,
	}
	if bytes, err := proto.Marshal(&responseData); err != nil {
		Log.Infof("SubscribeHandler error: %v", err)
		b.Return(conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
	} else {
		b.Return(conn, protocol.OpaqueType_Subscription, bytes, nil)
	}

}
func (b *Broker) SyncConsumeOffsetHandler(body []byte, conn net.Conn, requestId string) {
	req := &protocol.SyncConsumeOffsetRequestData{}
	if err := proto.Unmarshal(body, req); err != nil {
		b.Return(conn, protocol.OpaqueType_SyncConsumeOffset, nil, ErrRequest)
		return
	}
	Log.Infof("SyncConsumeOffsetHandler 收到同步消费进度请求 - [requestId:%s][clientId:%s][主题:%s][消费组:%s][队列:%d][偏移量:%d]", requestId, req.ClientId, req.Topic, req.ConsumerGroup, req.QueueId, req.Offset)
	// table中标记已经完整这一批消费
	if record, err := b.topicRouteManager.GetTopicTableRecord(req.Topic, req.ConsumerGroup, req.QueueId, req.ClientId); err != nil {
		Log.Infof("SyncTopicRouteInfoHandler - [requestId:%s] [error: %v]", requestId, err)
		return
	} else {
		if req.Offset > record.offset {
			record.offset = req.Offset
		}
		// 获取到当前的消息, 判断当前是否准备rebalance，如果是的把队列的锁交给rebalance
		key := b.GetReBalanceKey(req.Topic, req.ConsumerGroup)
		if flag, ok := b.reBalanceMap[key]; ok && flag == true {
			record.clientId = ReBalance
		}
	}
}

func (b *Broker) SyncTopicRouteInfoHandler(body []byte, conn net.Conn, requestId string) {
	req := &protocol.SyncTopicRouteRequestData{}
	if err := proto.Unmarshal(body, req); err != nil {
		b.Return(conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
		return
	}
	Log.Infof("SyncTopicRouteInfoHandler %s - 开始处理同步主题路由信息请求 %+v", requestId, req)
	queues := b.topicRouteManager.FindQueues(req.Topic, req.ConsumerGroup, req.ClientId)
	resp := protocol.SyncTopicRouteResponseData{}
	resp.ConsumeProgress = queues
	if bytes, err := proto.Marshal(&resp); err != nil {
		Log.Infof("SyncTopicRouteInfoHandler error: %v", err)
		b.ReturnWithRequestId(requestId, conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
	} else {
		b.ReturnWithRequestId(requestId, conn, protocol.OpaqueType_SyncTopicRouteInfo, bytes, nil)
		Log.Infof("SyncTopicRouteInfoHandler - ")
	}
	Log.Infof("SyncTopicRouteInfoHandler %s - 处理结束", requestId)
}

// 定时检查客户端的心跳情况，如果超时的话，打上标记
func (b *Broker) CheckHeartbeatHandler() {
	for {
		clearFlag := false
		for _, c := range b.clients {
			if c.LastHeartbeat.Add(HeartbeatTimeout).Before(time.Now()) && c.Status == 1 {
				Log.Infof("发现客户端超时了 ClientId:%s", c.ClientID)
				c.Status = 0
				clearFlag = true
				if err := b.RemoveClient(c.ClientID); err != nil {
					Log.Infof("删除客户端异常 error: %v", err)
				} else {
					delete(b.clients, c.Conn)
				}
			}
		}
		if clearFlag {

		}
		time.Sleep(time.Second)
	}
}

func (b *Broker) HeartbeatHandler(body []byte, conn net.Conn, requestId string) {
	req := &protocol.HeartbeatRequestData{}
	if err := proto.Unmarshal(body, req); err != nil {
		Log.Infof("HeartbeatHandler - error: [requestId:%s] %v", requestId, err)
		b.Return(conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
		return
	}
	if req.Topic == "" || req.ConsumerGroup == "" || req.ClientId == "" {
		Log.Infof("HeartbeatHandler - error: [requestId:%s] 参数异常", requestId)
		b.Return(conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
	}
	client, err := b.GetClient(req.Topic, req.ConsumerGroup, req.ClientId)
	if err != nil {
		Log.Infof("HeartbeatHandler error: [requestId:%s] %v", requestId, err)
		b.Return(conn, protocol.OpaqueType_SyncTopicRouteInfo, nil, err)
		return
	}
	// 更新客户端的
	client.Status = 1
	client.LastHeartbeat = time.Now()
	Log.Infof("HeartbeatHandler - 心跳更新成功 [requestId:%s] [%+v]", requestId, *client)
}

/**
找到对应的channel，进行读取
*/
func (b *Broker) PullMessageHandler(body []byte, conn net.Conn, requestId string) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := &protocol.PullMessageRequestData{}
	if err := proto.Unmarshal(body, data); err != nil {
		Log.Errorf("SubscribeHandler error: %v", err)
		return
	}
	Log.Infof("PullMessageHandler - 收到拉取消息的请求 [requestId:%s]", requestId)
	topicName := data.Topic
	clientId := data.ClientId
	consumerGroupName := data.ConsumerGroup
	pullSize := data.PullSize
	queueId := data.QueueId
	offset := data.Offset
	// 构建返回数据
	responseData := protocol.PullMessageResponseData{}
	responseData.Topic = topicName
	responseData.ConsumerGroup = consumerGroupName
	responseData.QueueId = queueId
	responseData.Messages = make([]*protocol.Message, 0)
	Log.Debugf("收到拉取数据请求：%+v", data)
	// 检查topic是否存在
	if topic, ok := b.topics[topicName]; !ok {
		//b.ReturnWithRequestId(requestId, conn, protocol.OpaqueType_PullMessage, nil, ErrMessageNotYet)
		goto ReturnPullMessageData
	} else {
		// 检查消费组是否存在
		if consumerGroup, ok := topic.consumerGroups[consumerGroupName]; !ok {
			Log.Infof("PullMessageHandler error: 消费组不存在")
			//b.Return(conn, protocol.OpaqueType_PullMessage, nil, ErrConsumerGroupNotExist)
			goto ReturnPullMessageData
		} else {
			if _, ok := consumerGroup.Clients[clientId]; !ok {
				Log.Errorf("PullMessageHandler error: ClientId不存在")
				//b.Return(conn, protocol.OpaqueType_PullMessage, nil, ErrRequest)
				goto ReturnPullMessageData
			}
			// 从路由标找到当前的client
			queueIds := b.topicRouteManager.FindQueueId(topicName, consumerGroup.GroupName, clientId)
			// 判断队列Id是否是分配的
			if !tools.ContainsInt(queueIds, queueId) {
				Log.Errorf("PullMessageHandler error: 队列不存在")
				//b.Return(conn, protocol.OpaqueType_PullMessage, nil, ErrQueueId)
				goto ReturnPullMessageData
			} else {
				// 找到之前分配的channel
				Log.Debugf("PullMessageHandler 获取分配的队列ID：%d", queueId)
				consumeQueue := b.topics[topicName].ConsumeQueues[queueId]
				Log.Debugf("ClientId: %s 要拉取 %d 条数据, offset:%d,  开始监听queue %d", clientId, pullSize, offset, consumeQueue.queueId)
				fetchDataChanDone := make(chan struct{})
				go func() {
					targetOffset := offset + int64(pullSize)
					for {
						if offset >= targetOffset {
							fetchDataChanDone <- struct{}{}
							break
						}
						select {
						case <-ctx.Done():
							Log.Infof("协程拉取时间超时，准备退出")
							return
						default:
							if consumeQueueIndex, err := consumeQueue.read(offset); err != nil {
								if errors.Is(err, ErrMessageNotYet) {
									// 如果没有找到消息，等待一秒
									Log.Debugf("暂时还没有消息，等待1秒")
									time.Sleep(time.Second)
									continue
								} else {
									Log.Errorf("PullMessageHandler error %+v", err)
									return
								}
							} else {
								Log.Infof("已经获得commitLogOffset为%d, 准备读取commitLog的消息", consumeQueueIndex.commitLogOffset)
								if message, err := b.CommitLog.ReadMessage(consumeQueueIndex.commitLogOffset); err != nil {
									Log.Errorf("读取CommitLog文件异常 error: %+v", err)
									return
								} else {
									Log.Debugf("PullMessageHandler 获取到了数据,msgId: %s", message.MsgId)
									consumeQueue.messageChan <- message
									offset += 1
								}
							}

						}
					}

				}()
				for {
					select {
					case message := <-consumeQueue.messageChan:
						Log.Debugf("开始组转消息， msgId: %s, QueueId: %d, QueueOffset:%d", message.MsgId, message.QueueId, message.QueueOffset)
						responseData.Messages = append(responseData.Messages, message)
					case <-fetchDataChanDone:
						goto ReturnPullMessageData
					case <-ctx.Done():
						Log.Infof("拉取时间到，准备返回")
						goto ReturnPullMessageData
					}
				}
			}

		}
	}

ReturnPullMessageData:

	responseData.Size = int32(len(responseData.Messages))
	if bytes, err := proto.Marshal(&responseData); err != nil {
		Log.Infof("PullMessageHandler error: %v", err)
	} else {
		b.ReturnWithRequestId(requestId, conn, protocol.OpaqueType_PullMessage, bytes, nil)
	}
}

func (b *Broker) GetTopic(topicName string) *Topic {
	b.topicsLock.Lock()
	defer b.topicsLock.Unlock()
	if t, ok := b.topics[topicName]; !ok {
		topic := NewTopic(topicName)
		b.topics[topicName] = topic
		_, err := b.topicsFile.Write([]byte(topicName))
		if err != nil {
			Log.Fatal("写入topic文件异常 err: ", err)
		}
		b.topicsFile.Sync()
		return b.topics[topicName]
	} else {
		return t
	}
}

func (b *Broker) GetTopicConsumerGroup(gname string, topic *Topic) *ConsumerGroup {
	b.consumerGroupLock.Lock()
	defer b.consumerGroupLock.Unlock()
	if group, ok := topic.consumerGroups[gname]; !ok {
		group = &ConsumerGroup{
			TopicName: topic.topicName,
			GroupName: gname,
			Clients:   make(map[string]*Client),
		}
		topic.consumerGroups[gname] = group
		return group
	} else {
		return group
	}
	return nil
}

// 生产者只管往某个主题生产消息
func (b *Broker) ProducerMessage(msg *protocol.Message) error {
	Log.Infof("ProducerMessage - [msgId:%s] 开始写入数据", msg.MsgId)
	// 根据消息的主题，选择队列
	topicName := msg.Topic
	topic := b.GetTopic(topicName)
	// 随机选择队列
	randQueueId := rand.Intn(len(topic.ConsumeQueues))
	Log.Infof("ProducerMessage - [msgId:%s] 选择队列 %d", msg.MsgId, randQueueId)
	consumeQueue := topic.ConsumeQueues[randQueueId]
	msg.QueueId = int32(consumeQueue.queueId)
	bodyBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if queueOffset, err := consumeQueue.write(b.CommitLog.currentOffset, int32(len(bodyBytes)), 1); err != nil {
		Log.Fatal("ProducerMessage error: ", err)
	} else {
		msg.QueueOffset = queueOffset
		if commitLogOffset, err := b.CommitLog.WriteMessage(msg); err != nil {
			Log.Fatal("ProducerMessage error: ", err)
		} else {
			Log.Infof("写入消息成功，commitLogOffset%d, queueOffset:%d", commitLogOffset, queueOffset)
		}
	}

	return nil
}

func (b *Broker) init() {

	// 初始化log
	// 设置日志级别为 Debug，并将日志输出到标准输出
	Log.SetLevel(LogLevel)
	Log.SetOutput(os.Stdout)
	//Log.SetReportCaller(true)

	// 初始化commitLog引擎
	if commitLog, err := NewCommitLog(); err != nil {
		Log.Fatal(err)
	} else {
		b.CommitLog = commitLog
	}
	// 初始化topic
	// 读取topic文件，并且写入
	b.topicRouteManager = &TopicRouteManager{
		flushLock: sync.Mutex{},
	}
	topics, err := b.topicRouteManager.Init()
	if err != nil {
		Log.Fatal(err)
	} else {
		b.topics = topics
	}
	// 开启消费组的平衡协程
	for _, tp := range topics {
		for _, cg := range tp.consumerGroups {
			go b.doReBalance(tp.topicName, cg.GroupName)
		}
	}

	Log.Infof("初始化完成......")
}

func (b *Broker) Return(conn net.Conn, opaque protocol.OpaqueType, dataBytes []byte, err error) {
	var remotingCommand protocol.RemotingCommand
	if err != nil {
		remotingCommand = protocol.RemotingCommand{
			Type: protocol.RemotingCommandType_ResponseCommand,
			Header: &protocol.RemotingCommandHeader{
				Code:   400,
				Opaque: opaque,
				Flag:   0,
				Remark: "",
			},
			Body: []byte(err.Error()),
		}
	} else {
		remotingCommand = protocol.RemotingCommand{
			Type: protocol.RemotingCommandType_ResponseCommand,
			Header: &protocol.RemotingCommandHeader{
				Code:   200,
				Opaque: opaque,
				Flag:   0,
				Remark: "",
			},
			Body: dataBytes,
		}
	}
	// 从上下文中获取全局变量
	if value, ok := context.Background().Value(requestIdKey).(string); ok {
		remotingCommand.RequestId = value
	}
	if remotingCommandBytes, err := proto.Marshal(&remotingCommand); err != nil {
		Log.Infof("PullMessageHandler error: %v", err)
	} else {
		if _, err := conn.Write(remotingCommandBytes); err != nil {
			Log.Infof("Failed to send response: %v\n", err)
		}
	}
}

func (b *Broker) ReturnWithRequestId(requestId string, conn net.Conn, opaque protocol.OpaqueType, dataBytes []byte, err error) {
	go func() {
		var remotingCommand protocol.RemotingCommand
		if err != nil {
			remotingCommand = protocol.RemotingCommand{
				Type: protocol.RemotingCommandType_ResponseCommand,
				Header: &protocol.RemotingCommandHeader{
					Code:   400,
					Opaque: opaque,
					Flag:   0,
					Remark: "",
				},
				Body: []byte(err.Error()),
			}
		} else {
			remotingCommand = protocol.RemotingCommand{
				Type: protocol.RemotingCommandType_ResponseCommand,
				Header: &protocol.RemotingCommandHeader{
					Code:   200,
					Opaque: opaque,
					Flag:   0,
					Remark: "",
				},
				Body:      dataBytes,
				RequestId: requestId,
			}
		}
		// 从上下文中获取全局变量
		if remotingCommandBytes, err := proto.Marshal(&remotingCommand); err != nil {
			Log.Infof("PullMessageHandler error: %v", err)
		} else {
			if _, err := conn.Write(remotingCommandBytes); err != nil {
				Log.Infof("Failed to send response: %v", err)
			}
		}
	}()

}

func (b *Broker) GetClient(topicName string, consumerGroupName string, clientId string) (*Client, error) {
	if topic, ok := b.topics[topicName]; !ok {
		return nil, ErrConsumerGroupNotExist
	} else {
		// 检查消费组是否存在
		if consumerGroup, ok := topic.consumerGroups[consumerGroupName]; !ok {
			return nil, ErrConsumerGroupNotExist
		} else {
			if client, ok := consumerGroup.Clients[clientId]; !ok {
				return nil, ErrClientNotExist
			} else {
				return client, nil
			}
		}
	}
}

func (b *Broker) reBalance(topicName string, consumerGroupName string) {
	key := b.GetReBalanceKey(topicName, consumerGroupName)
	b.reBalanceMap[key] = true
}

// 重新分配队列
// 能不能每一个消费组创建一个自动平台的goroutine呢
func (b *Broker) doReBalance(topicName string, consumerGroupName string) {
	Log.Infof("开启消费组的平衡协程 %s %s", topicName, consumerGroupName)
	for {
		Log.Debugf("doReBalance - 检测[topic:%s][consumerGroupName:%s]", topicName, consumerGroupName)
		key := b.GetReBalanceKey(topicName, consumerGroupName)
		if flag, ok := b.reBalanceMap[key]; !ok {
			time.Sleep(3 * time.Second)
			continue
		} else {
			if !flag {
				time.Sleep(3 * time.Second)
				continue
			}
		}

		// 判断是否拿到所有的锁
		records := b.topicRouteManager.GetAllConsumerGroupRecord(topicName, consumerGroupName)
		if len(records) > 0 {
			ok := true
			for _, record := range records {
				if record.clientId != ReBalance {
					ok = false
				}
			}
			if !ok {
				time.Sleep(3 * time.Second)
				continue
			}
		}

		Log.Infof("doReBalance - 开始重新分配队列")

		// 获取正常可用的客户端
		var clients []*Client
		for _, c := range b.topics[topicName].consumerGroups[consumerGroupName].Clients {
			if c.Status == 1 {
				clients = append(clients, c)
			}
		}
		topicRouteRecordsSize := len(records)
		clientsSize := len(clients)
		if clientsSize < 1 {
			time.Sleep(3 * time.Second)
			continue
		}
		Log.Infof("ReBalance - topicRouteRecordsSize：%d, clientsSize:%d", topicRouteRecordsSize, clientsSize)
		//重新分配
		//分配策略: 获取现在所有的消费组中所有的clientId，做平均分配
		var targetClients []*Client
		// 如果队列的数量比客户端多，先每个客户端分配一个，然后随机冲clients抽计入进去
		// todo 简单的分配策略
		if topicRouteRecordsSize >= clientsSize {
			targetClients = append(targetClients, clients...)
			for i := 0; i < topicRouteRecordsSize-clientsSize; i++ {
				targetIndex := rand.Intn(len(clients))
				targetClients = append(targetClients, clients[targetIndex])
			}
		} else {
			for i := 0; i < clientsSize; i++ {
				targetClients = append(targetClients, clients[i])
			}
		}
		for i, r := range records {
			r.clientId = targetClients[i].ClientID
		}

		// 平衡之后
		b.reBalanceMap[key] = false
	}

}

func (b *Broker) GetReBalanceKey(topicName string, consumerGroupName string) string {
	return fmt.Sprintf("%s#%s", topicName, consumerGroupName)
}

func (b *Broker) RemoveClient(clientId string) error {
	Log.Infof("开始删除Client:%s", clientId)
	// 删除掉topic中的client
	Log.Infof("删除掉topic中的client:%s", clientId)

	var reBalanceTarget []*ConsumerGroup //group->topic

	for _, topic := range b.topics {
		for _, c := range topic.consumerGroups {
			if _, ok := c.Clients[clientId]; ok {
				// 删除
				delete(c.Clients, clientId)
				reBalanceTarget = append(reBalanceTarget, c)
			}
		}
	}
	Log.Infof("删除topicTable中的client:%s", clientId)
	// 删除topicTable中的client
	for _, r := range b.topicRouteManager.table {
		if r.clientId == clientId {
			r.clientId = ReBalance
		}
	}
	if len(reBalanceTarget) > 0 {
		for _, c := range reBalanceTarget {
			b.reBalance(c.TopicName, c.GroupName)
		}
	}
	return nil
}
