package core

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/erixyuan/go-titan-mq/tools"
	"github.com/golang/protobuf/proto"
	"github.com/julienschmidt/httprouter"
	"io"
	"log"
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

type Broker struct {
	clients            map[net.Conn]string // 方便从conn找到clientId
	topics             map[string]*Topic
	topicsLock         sync.Mutex
	consumerGroupLock  sync.Mutex
	consumerClientLock sync.Mutex
	commitLogMutex     sync.Mutex
	CommitLog          *CommitLog
	topicsFile         *os.File
	topicRouteManager  *TopicRouteManager
}

func NewBroker() *Broker {
	broker := Broker{
		clients:            make(map[net.Conn]string),
		topics:             make(map[string]*Topic),
		topicsLock:         sync.Mutex{},
		consumerGroupLock:  sync.Mutex{},
		consumerClientLock: sync.Mutex{},
		commitLogMutex:     sync.Mutex{},
	}
	return &broker
}

func (b *Broker) Start(port int) error {
	fmt.Printf("Broker 开始初始化................................")
	b.init()
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer tcpListener.Close()
	// 开启tcp的监听
	fmt.Printf("Broker 开始初始化监听端口 port %d\n", port)
	go b.tcpListenerHandler(tcpListener)

	// 监听本地的HTTP连接
	httpListener, _ := net.Listen("tcp", ":9090")
	defer httpListener.Close()

	fmt.Println("开始创建http服务")
	// 创建HTTP服务器
	router := httprouter.New()
	handler := HttpHandler{
		topicRouteManager: b.topicRouteManager,
	}
	router.GET("/", handler.Index)
	router.POST("/topic/add", handler.AddTopic)
	router.POST("/consumerGroup/add", handler.AddConsumerGroup)
	router.POST("/topic/db", handler.FetchTopicDb)
	router.GET("/topic/data", handler.FetchTopicData)
	router.GET("/topic/table", handler.FetchTopicTable)

	// 启动两个监听器
	err = http.ListenAndServe(":9091", router)
	if err != nil {
		log.Fatal(err)
	}
	select {}
	return nil
}

func (b *Broker) tcpListenerHandler(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Println("New client connected:", conn.RemoteAddr())

		go b.ListeningClient(conn)
		//go b.ProducerMessage()
	}
}

func (b *Broker) ListeningClient(conn net.Conn) {
	for {
		buf := make([]byte, 1024*4)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("Failed to read request from client: %v\n", err)
			if err == io.EOF {
				fmt.Printf("%s连接断开了，剔除client", conn.RemoteAddr())
				b.topicRouteManager.RemoveClient(b.clients[conn])
			}
			return
		}
		remotingCommand := &protocol.RemotingCommand{}
		err = proto.Unmarshal(buf[:n], remotingCommand)
		if err != nil {
			log.Printf("收到请求体，内容异常: error +%v", err)
			continue
		}
		if remotingCommand.Type == protocol.RemotingCommandType_RequestCommand {
			log.Printf("收到请求：%+v", &remotingCommand.Header)
			// 如果是请求
			switch remotingCommand.Header.Opaque {
			//case protocol.OpaqueType_RegisterConsumer:
			//go b.RegisterConsumer(remotingCommand.Body, conn)
			case protocol.OpaqueType_SyncTopicRouteInfo:
				// 同步路由信息
				go b.SyncTopicRouteInfo(remotingCommand.Body, conn)
			case protocol.OpaqueType_Subscription:
				// 订阅消息
				go b.SubscribeHandler(remotingCommand.Body, conn)
			case protocol.OpaqueType_Unsubscription:
				//
			case protocol.OpaqueType_Publish:
			case protocol.OpaqueType_PullMessage:
				go b.PullMessageHandler(remotingCommand.Body, conn)
			}
		} else if remotingCommand.Type == protocol.RemotingCommandType_ResponseCommand {
			// 如果是响应，例如消息确认
			log.Printf("收到响应：%+v", remotingCommand.Header)
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
	log.Printf("收到订阅请求")
	req := protocol.SubscriptionRequestData{}
	if err := proto.Unmarshal(body, &req); err != nil {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_Subscription, nil, ErrRequest)
		return
	}
	log.Printf("收到订阅请求，数据为：%+v", req)
	topicName := req.Topic
	clientId := req.ClientId
	consumerGroupName := req.ConsumerGroup
	// 检查topic是否存在
	if topic, ok := b.topics[topicName]; !ok {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_Subscription, nil, ErrMessageNotYet)
		return
	} else {
		// 检查消费组是否存在
		if consumerGroup, ok := topic.consumerGroups[consumerGroupName]; !ok {
			log.Printf("SubscribeHandler error: 消费组[%s]不存在", consumerGroupName)
			b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_Subscription, nil, ErrConsumerGroupNotExist)
			return
		} else {
			if _, ok := consumerGroup.Clients[clientId]; ok {
				// 已经存在了就返回订阅异常
				log.Printf("SubscribeHandler error: 消费组中的ClientId已经存在")
				b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_Subscription, nil, ErrClientExist)
				return
			} else {
				// 注册消费者
				if err := b.topicRouteManager.RegisterConsumer(topicName, consumerGroupName, clientId, conn); err != nil {
					// 登记入当前的客户端表，方便查询
					b.clients[conn] = clientId
				}
			}
		}
	}

	queueIds := b.topicRouteManager.FindQueueId(req.Topic, req.ConsumerGroup, req.ClientId)
	// 返回
	responseData := protocol.SubscriptionResponseData{
		Topic:         topicName,
		ClientId:      clientId,
		ConsumerGroup: consumerGroupName,
		QueueIds:      queueIds,
	}
	if bytes, err := proto.Marshal(&responseData); err != nil {
		log.Printf("SubscribeHandler error: %v", err)
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
	} else {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_Subscription, bytes, nil)
	}

}
func (b *Broker) SyncTopicRouteInfo(body []byte, conn net.Conn) {
	req := &protocol.SyncTopicRouteRequestData{}
	if err := proto.Unmarshal(body, req); err != nil {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
		return
	}
	queueIds := b.topicRouteManager.FindQueueId(req.Topic, req.ConsumerGroup, req.ClientId)
	resp := protocol.SyncTopicRouteResponseData{}
	resp.QueueId = queueIds
	if bytes, err := proto.Marshal(&resp); err != nil {
		log.Printf("SyncTopicRouteInfo error: %v", err)
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_SyncTopicRouteInfo, nil, ErrRequest)
	} else {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_SyncTopicRouteInfo, bytes, nil)
	}
}

/**
找到对应的channel，进行读取
*/
func (b *Broker) PullMessageHandler(body []byte, conn net.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data := &protocol.PullMessageRequestData{}
	if err := proto.Unmarshal(body, data); err != nil {
		log.Printf("SubscribeHandler error: %v", err)
		return
	}
	topicName := data.Topic
	clientId := data.ClientId
	consumerGroupName := data.ConsumerGroup
	pullSize := data.PullSize
	queueId := data.QueueId
	offset := data.Offset
	log.Printf("收到拉取数据请求：%+v", data)
	// 检查topic是否存在
	if topic, ok := b.topics[topicName]; !ok {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_PullMessage, nil, ErrMessageNotYet)
	} else {
		// 检查消费组是否存在
		if consumerGroup, ok := topic.consumerGroups[consumerGroupName]; !ok {
			log.Printf("PullMessageHandler error: 消费组不存在")
			b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_PullMessage, nil, ErrConsumerGroupNotExist)
			return
		} else {
			if _, ok := consumerGroup.Clients[clientId]; !ok {
				log.Printf("PullMessageHandler error: ClientId不存在")
				b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_PullMessage, nil, ErrRequest)
				return
			}
			// 从路由标找到当前的client
			queueIds := b.topicRouteManager.FindQueueId(topicName, consumerGroup.GroupName, clientId)
			// 判断队列Id是否是分配的
			if !tools.ContainsInt(queueIds, queueId) {
				b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_PullMessage, nil, ErrQueueId)
				return
			}
		}
	}

	// 找到之前分配的channel
	log.Printf("PullMessageHandler 获取分配的队列ID：%+v", b.topics[topicName].ConsumeQueues)
	log.Printf("PullMessageHandler 获取分配的队列ID：%d", queueId)
	consumeQueue := b.topics[topicName].ConsumeQueues[queueId]

	// 构建返回数据
	responseData := protocol.PullMessageResponseData{}
	responseData.Topic = topicName
	responseData.ConsumerGroup = consumerGroupName
	responseData.Messages = make([]*protocol.Message, 0)
	log.Printf("ClientId: %s 要拉取 %d 条数据, offset:%d,  开始监听queue %d", clientId, pullSize, offset, consumeQueue.queueId)

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
				fmt.Println("协程拉取时间超时，准备退出")
				return
			default:
				if consumeQueueIndex, err := consumeQueue.read(offset); err != nil {
					if errors.Is(err, ErrMessageNotYet) {
						// 如果没有找到消息，等待一秒
						log.Printf("暂时还没有消息，等待1秒")
						time.Sleep(time.Second)
						continue
					} else {
						log.Printf("PullMessageHandler error %+v", err)
						return
					}
				} else {
					log.Printf("已经获得commitLogOffset为%d, 准备读取commitLog的消息", consumeQueueIndex.commitLogOffset)
					if message, err := b.CommitLog.ReadMessage(consumeQueueIndex.commitLogOffset); err != nil {
						log.Printf("读取CommitLog文件异常 error: %+v", err)
						return
					} else {
						fmt.Println("PullMessageHandler 获取到了数据,msgId:", message.MsgId)
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
			log.Printf("开始组转消息， msgId: %s, QueueOffset:%d", message.MsgId, message.QueueOffset)
			responseData.Messages = append(responseData.Messages, message)
		case <-fetchDataChanDone:
			goto ReturnPullMessageData
		case <-ctx.Done():
			fmt.Println("拉取时间到，准备返回")
			goto ReturnPullMessageData
		}
	}
ReturnPullMessageData:

	responseData.Size = int32(len(responseData.Messages))
	if bytes, err := proto.Marshal(&responseData); err != nil {
		log.Printf("PullMessageHandler error: %v", err)
	} else {
		b.Return(conn, protocol.RemotingCommandType_ResponseCommand, protocol.OpaqueType_PullMessage, bytes, nil)
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
			log.Fatal("写入topic文件异常 err: ", err)
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
	log.Printf("%s开始写入数据", msg.MsgId)
	// 根据消息的主题，选择队列
	topicName := msg.Topic
	topic := b.GetTopic(topicName)
	// 随机选择队列
	randQueueId := rand.Intn(len(topic.ConsumeQueues))
	log.Printf("%s 选择队列 %d", msg.MsgId, randQueueId)
	consumeQueue := topic.ConsumeQueues[randQueueId]
	bodyBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	if queueOffset, err := consumeQueue.write(b.CommitLog.currentOffset, int32(len(bodyBytes)), 1); err != nil {
		log.Fatal("ProducerMessage error: ", err)
	} else {
		msg.QueueOffset = queueOffset
		if commitLogOffset, err := b.CommitLog.WriteMessage(msg); err != nil {
			log.Fatal("ProducerMessage error: ", err)
		} else {
			log.Printf("写入消息成功，commitLogOffset%d, queueOffset:%d", commitLogOffset, queueOffset)
		}
	}

	return nil
}

/**
通过offset读取文件，然后发给消费者
*/
func (b *Broker) readCommitLogByOffset(offset int64) {
	// 打开文件
	fileName := "largefile.txt"
	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 设置文件偏移量
	whence := io.SeekStart
	pos, err := file.Seek(offset, whence)
	if err != nil {
		panic(err)
	}
	fmt.Printf("当前文件偏移量：%d\n", pos)
	// 读取文件数据
	buf := make([]byte, 1024)
	n, err := file.Read(buf)
	if err != nil {
		panic(err)
	}
	fmt.Printf("实际读取的字节数：%d\n", n)
	fmt.Printf("读取的文件数据：%s\n", string(buf[:n]))

}

/**
用二分查找法，找到对应的消息
*/
func (b *Broker) readConsumeQueueByOffset(consumeQueueFilePath string, messageOffset int64) {
	// 打开消费进度文件
	file, err := os.Open(consumeQueueFilePath)
	if err != nil {
		fmt.Printf("Failed to open consume queue file: %v\n", err)
		return
	}
	defer file.Close()
	// 二分查找消息偏移量
	var left, right, mid int64
	fileInfo, _ := file.Stat()
	fileSize := fileInfo.Size()
	left = 0
	right = fileSize/20 - 1 // 计算消息条目数量
	for left <= right {
		mid = (left + right) / 2
		messageOffsetPos := mid * 20
		messageOffsetBytes := make([]byte, 8)
		if _, err := file.ReadAt(messageOffsetBytes, messageOffsetPos+4); err != nil {
			fmt.Printf("Failed to read message offset from consume queue file: %v\n", err)
			return
		}
		curMessageOffset := int64(binary.BigEndian.Uint64(messageOffsetBytes))
		if curMessageOffset < messageOffset {
			left = mid + 1
		} else if curMessageOffset > messageOffset {
			right = mid - 1
		} else {
			// 找到消息
			consumeStatusBytes := make([]byte, 1)
			if _, err := file.ReadAt(consumeStatusBytes, messageOffsetPos+16); err != nil {
				fmt.Printf("Failed to read consume status from consume queue file: %v\n", err)
				return
			}
			consumeStatus := consumeStatusBytes[0]
			fmt.Printf("Message offset %v consume status is %v\n", messageOffset, consumeStatus)
			return
		}
	}
	fmt.Printf("Message offset %v not found in consume queue file\n", messageOffset)
}

func (b *Broker) init() {

	// 初始化commitLog引擎
	if commitLog, err := NewCommitLog(); err != nil {
		log.Fatal(err)
	} else {
		b.CommitLog = commitLog
	}

	// 初始化topic
	// 读取topic文件，并且写入
	b.topicRouteManager = &TopicRouteManager{}
	topics, err := b.topicRouteManager.Init()
	if err != nil {
		log.Fatal(err)
	} else {
		b.topics = topics
	}
	fmt.Println("初始化完成......")
}

func (b *Broker) Return(conn net.Conn, commandType protocol.RemotingCommandType, opaque protocol.OpaqueType, dataBytes []byte, err error) {
	var remotingCommand protocol.RemotingCommand
	if err != nil {
		remotingCommand = protocol.RemotingCommand{
			Type: commandType,
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
			Type: commandType,
			Header: &protocol.RemotingCommandHeader{
				Code:   200,
				Opaque: opaque,
				Flag:   0,
				Remark: "",
			},
			Body: dataBytes,
		}
	}
	if remotingCommandBytes, err := proto.Marshal(&remotingCommand); err != nil {
		log.Printf("PullMessageHandler error: %v", err)
	} else {
		if _, err := conn.Write(remotingCommandBytes); err != nil {
			fmt.Printf("Failed to send response: %v\n", err)
		}
	}

}
