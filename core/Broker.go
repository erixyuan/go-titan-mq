package core

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
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

var (
	ErrMessageNotFound = errors.New("找不到消息")
	ErrMessageNotYet   = errors.New("还没有消息")
)

type Broker struct {
	clients            map[net.Conn]bool
	topics             map[string]*Topic
	topicsLock         sync.Mutex
	consumerGroupLock  sync.Mutex
	consumerClientLock sync.Mutex
	commitLogMutex     sync.Mutex
	CommitLog          *CommitLog
	topicsFile         *os.File
}

func NewBroker() *Broker {
	broker := Broker{
		clients:            make(map[net.Conn]bool),
		topics:             make(map[string]*Topic),
		topicsLock:         sync.Mutex{},
		consumerGroupLock:  sync.Mutex{},
		consumerClientLock: sync.Mutex{},
		commitLogMutex:     sync.Mutex{},
	}
	return &broker
}

func (b *Broker) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Broker 开始初始化................................")
	b.init()
	fmt.Printf("Broker 开始初始化监听端口 port %d\n", port)

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
	b.clients[conn] = true
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("Failed to read request from client: %v\n", err)
			return
		}
		remotingCommand := &protocol.RemotingCommand{}
		err = proto.Unmarshal(buf[:n], remotingCommand)
		if remotingCommand.Type == protocol.RemotingCommandType_RequestCommand {
			log.Printf("收到请求：%+v", remotingCommand.Header)
			// 如果是请求
			switch remotingCommand.Header.Opaque {
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
	data := &protocol.SubscriptionRequestData{}
	if err := proto.Unmarshal(body, data); err != nil {
		log.Printf("SubscribeHandler error: %v", err)
		return
	}
	log.Printf("收到订阅请求，数据为：%+v", data)
	topic := data.Topic
	clientId := data.ClientId
	consumerGroupName := data.ConsumerGroup
	// 获取topic实体
	thisTopic := b.GetTopic(topic)
	// 获取消费组实体
	consumerGroup := b.GetTopicConsumerGroup(consumerGroupName, thisTopic)
	// 进行订阅
	if _, ok := consumerGroup.Clients[clientId]; !ok {
		client := Client{
			Conn:          conn,
			ClientID:      clientId,
			LastHeartbeat: time.Now(),
			Status:        1,
			Queue:         thisTopic.queues[rand.Intn(len(thisTopic.queues))], // 随机分配队列
		}
		consumerGroup.Clients[clientId] = &client
		log.Printf("订阅成功 %+v", client)
	}

	// 返回
	responseData := protocol.SubscriptionResponseData{
		Topic:         topic,
		ClientId:      clientId,
		ConsumerGroup: consumerGroupName,
	}
	if bytes, err := proto.Marshal(&responseData); err != nil {
		log.Printf("SubscribeHandler error: %v", err)
	} else {
		remotingCommand := protocol.RemotingCommand{
			Type: protocol.RemotingCommandType_ResponseCommand,
			Header: &protocol.RemotingCommandHeader{
				Code:   200,
				Opaque: protocol.OpaqueType_Subscription,
				Flag:   0,
				Remark: "",
			},
			Body: bytes,
		}
		if remotingCommandBytes, err := proto.Marshal(&remotingCommand); err != nil {
			log.Printf("SubscribeHandler error: %v", err)
		} else {
			if _, err := conn.Write(remotingCommandBytes); err != nil {
				fmt.Printf("Failed to send response: %v\n", err)
			} else {
				log.Printf("返回订阅消息成功")
			}
		}
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
	topic := data.Topic
	clientId := data.ClientId
	consumerGroupName := data.ConsumerGroup
	pullSize := data.PullSize
	queueId := data.QueueId
	offset := data.Offset

	// 找到之前分配的channel
	consumeQueue := b.topics[topic].ConsumeQueues[queueId]

	// 构建返回数据
	responseData := protocol.PullMessageResponseData{}
	responseData.Topic = topic
	responseData.ConsumerGroup = consumerGroupName
	responseData.Messages = make([]*protocol.Message, 0)
	log.Printf("ClientId: %s 要拉取 %d 条数据, 开始监听chanel %+v", clientId, pullSize, consumeQueue)

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
		remotingCommand := protocol.RemotingCommand{
			Type: protocol.RemotingCommandType_ResponseCommand,
			Header: &protocol.RemotingCommandHeader{
				Code:   200,
				Opaque: protocol.OpaqueType_PullMessage,
				Flag:   0,
				Remark: "",
			},
			Body: bytes,
		}
		log.Printf("responseData md5:%d", md5.Sum(bytes))
		if remotingCommandBytes, err := proto.Marshal(&remotingCommand); err != nil {
			log.Printf("PullMessageHandler error: %v", err)
		} else {
			if _, err := conn.Write(remotingCommandBytes); err != nil {
				fmt.Printf("Failed to send response: %v\n", err)
			} else {
				log.Printf("返回拉数据的结果成功, 数量为 %d", responseData.Size)
			}
		}
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
	// 根据消息的主题，选择队列
	topicName := msg.Topic
	topic := b.GetTopic(topicName)
	// tod 随机选择队列
	randQueueId := rand.Intn(len(topic.ConsumeQueues))
	consumeQueue := topic.ConsumeQueues[randQueueId]
	// todo 这里要计算消息的长度
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
	topicFile, err := os.OpenFile("./store/topic.db", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}
	b.topicsFile = topicFile
	scanner := bufio.NewScanner(b.topicsFile)
	for scanner.Scan() {
		topicName := scanner.Text()
		if topicName != "" {
			fmt.Println("读取到topic", scanner.Text())
			topic := NewTopic(topicName)
			b.topics[topicName] = topic
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Scan file error:", err)
	}
	fmt.Println("初始化完成......")
}
