package core

import (
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

type Broker struct {
	clients            map[net.Conn]bool
	commitLog          map[string]*os.File
	topics             map[string]*Topic
	topicsLock         sync.Mutex
	consumerGroupLock  sync.Mutex
	consumerClientLock sync.Mutex
	commitLogMutex     sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		clients:            make(map[net.Conn]bool),
		commitLog:          make(map[string]*os.File),
		topics:             make(map[string]*Topic),
		topicsLock:         sync.Mutex{},
		consumerGroupLock:  sync.Mutex{},
		consumerClientLock: sync.Mutex{},
		commitLogMutex:     sync.Mutex{},
	}
}

func (b *Broker) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Broker listening on port %d\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Println("New client connected:", conn.RemoteAddr())

		go b.ListeningClient(conn)
		go b.ProducerMessage()
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
	data := &protocol.PullMessageRequestData{}
	if err := proto.Unmarshal(body, data); err != nil {
		log.Printf("SubscribeHandler error: %v", err)
		return
	}
	topic := data.Topic
	clientId := data.ClientId
	consumerGroupName := data.ConsumerGroup
	pullSize := data.PullSize

	// 找到之前分配的channel
	queueChannel := b.topics[topic].consumerGroups[consumerGroupName].Clients[clientId].Queue

	// 构建返回数据
	responseData := protocol.PullMessageResponseData{}
	responseData.Topic = topic
	responseData.ConsumerGroup = consumerGroupName
	responseData.Messages = make([]*protocol.Message, 0)
	log.Printf("ClientId: %s 要拉取 %d 条数据, 开始监听chanel %+v", clientId, pullSize, queueChannel)
	for i := 0; i < int(pullSize); i++ {
		select {
		case message := <-queueChannel:
			responseData.Messages = append(responseData.Messages, &message)
		case <-time.After(10 * time.Second):
			fmt.Println("Timeout, returning partial response.")
			break
		}
	}

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
		m1 := make(map[string]*ConsumerGroup)
		m2 := make(map[string]*ProducerGroup)
		q := make([]chan protocol.Message, 0)
		q = append(q, make(chan protocol.Message, 10))
		b.topics[topicName] = &Topic{
			topicName:      topicName,
			consumerGroups: m1,
			producerGroups: m2,
			queues:         q, // 默认3个队列
		}
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

func (b *Broker) ProducerMessage() {
	count := 1
	for {
		time.Sleep(time.Second)
		for name, topic := range b.topics {
			intn := rand.Intn(len(topic.queues))
			messagesCh := topic.queues[intn]
			log.Printf("开始生产消息 topic:%s, 队列长度: %d, ch: %+v", topic.topicName, len(topic.queues), messagesCh)
			messagesCh <- protocol.Message{
				Topic:          name,
				Body:           []byte(fmt.Sprintf("这是来自queue%d的消息", intn)),
				BornTimestamp:  0,
				StoreTimestamp: 0,
				MsgId:          GenerateSerialNumber("M"),
				ProducerGroup:  "GID-P-01",
				ConsumerGroup:  "GID-C-01",
			}
		}
		count += 1
	}
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

//
//// 创建commit log 的topic文件
//func (b *Broker) createTopicCommitLogFile(topic string) *os.File {
//	b.commitLogMutex.Lock()
//	defer b.commitLogMutex.Unlock()
//	filePath := fmt.Sprintf("%s.topic", topic)
//	_, err := os.Stat(filePath)
//	var file *os.File
//	if os.IsNotExist(err) {
//		// If the file does not exist, create it
//		file, err = os.Create(filePath)
//		if err != nil {
//			fmt.Println("Failed to create file:", err)
//			if file, err = os.OpenFile(fmt.Sprintf(filePath, topic), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err != nil {
//				fmt.Println("Failed to get file:", err)
//			} else {
//				b.commitLog[topic] = file
//			}
//		} else {
//			b.commitLog[topic] = file
//		}
//		return file
//	} else {
//		if file, err = os.OpenFile(fmt.Sprintf("%s.log", topic), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err != nil {
//			fmt.Println("Failed to get file:", err)
//		} else {
//			b.commitLog[topic] = file
//		}
//		return file
//	}
//}
//
//func (b *Broker) getTopicCommitLogFile(topic string) *os.File {
//	if file, ok := b.commitLog[topic]; ok {
//		return file
//	} else {
//		return b.createTopicCommitLogFile(topic)
//	}
//}
//
//func (b *Broker) storeMsgToCommitLog(coreMsg *coreMessage) {
//
//	if str, err := json.Marshal(coreMsg); err != nil {
//		log.Printf("storeMsgToCommitLog json.Marshal error %+v", err)
//	} else {
//		if _, err := b.getTopicCommitLogFile(coreMsg.Topic).WriteString(string(str) + "\n"); err != nil {
//			log.Printf("storeMsgToCommitLog WriteString error %+v, str is %s", err, str)
//		} else {
//			log.Printf("storeMsgToCommitLog success %s", str)
//			if err := b.getTopicCommitLogFile(coreMsg.Topic).Sync(); err != nil {
//				log.Printf("storeMsgToCommitLog Sync error %+v", err)
//			}
//		}
//	}
//}
