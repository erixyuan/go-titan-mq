package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/broker"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

var ok = "ok"
var subscribe = "subscribe"

type TitanConsumerClient struct {
	address           string
	timeout           time.Duration
	retryTime         time.Duration
	conn              net.Conn
	callback          map[string]func(broker.Message)
	acceptIsOpen      bool
	clientId          string
	consumerGroupName string
	topic             string
	queues            []*protocol.ConsumeProgress
	queueOffset       map[int32]int64
	pullMessageSize   int32
	NextConsumeOffset int64 // 下一个消费的offset
	pullMessageLock   bool
	queueOffsetLock   sync.Mutex
}

// 创建客户端
func (t *TitanConsumerClient) Init(address string, topic string, name string) {
	t.address = address
	t.timeout = 1 * time.Second
	t.retryTime = 1 * time.Second
	t.callback = make(map[string]func(broker.Message))
	t.acceptIsOpen = false
	t.topic = topic
	t.clientId = GenerateSerialNumber("CLIENT")
	t.consumerGroupName = name
	t.pullMessageSize = 5
	t.queueOffset = make(map[int32]int64)
	t.queueOffsetLock = sync.Mutex{}
}

// 设置超时时间
func (t *TitanConsumerClient) SetTimeout(timeout int) {
	t.timeout = time.Duration(timeout) * time.Second
}

func (t *TitanConsumerClient) Start() error {
	log.Printf("开始连接地址：%s", t.address)
	conn, err := net.DialTimeout("tcp", t.address, t.timeout)
	if err != nil {
		return err
	}

	// 设置读写的超时时间
	//if err = conn.SetDeadline(time.Now().Add(t.timeout)); err != nil {
	//	return err
	//}
	t.conn = conn
	t.pullMessageLock = true
	// 开协程进行循环读
	if t.acceptIsOpen == false {
		t.accept()
		t.acceptIsOpen = true
		go t.PullMessage() // 只开启一次
	}
	if err = t.Subscribe(); err != nil {
		log.Fatalf("订阅失败")
	}
	return nil
}

// 订阅
func (t *TitanConsumerClient) Subscribe() error {
	body := protocol.SubscriptionRequestData{
		Topic:         t.topic,
		ConsumerGroup: t.consumerGroupName,
		ClientId:      t.clientId,
	}
	//log.Printf("准备订阅消息%+v", body)
	if bodyBytes, err := proto.Marshal(&body); err != nil {
		log.Printf("Subscribe error: %v", err)
	} else {
		go t.SendCommand(protocol.OpaqueType_Subscription, bodyBytes)
	}
	log.Printf("发送订阅消息成功，等待回复 %+v", body)
	return nil
}

func (t *TitanConsumerClient) Public(topic string, content string) error {
	return t.send(topic, content)
}

func (t *TitanConsumerClient) send(topic string, payload string) error {
	message := broker.Message{
		Topic:   topic,
		Payload: payload,
	}
	s, _ := json.Marshal(message)
	b := string(s)
	//log.Println("发送消息：", b)
	if _, err := fmt.Fprintln(t.conn, b); err != nil {
		return err
	}
	return nil
}

// 开启一个goroutine，等待数据过来
// 超时情况：等待过程中，如果连接返回读取超时，继续循环等待
// 如果连接
func (t *TitanConsumerClient) accept() {
	go func() {
		//// Continuously listen for messages from the message broker
		log.Printf("开始等待消息返回 %s %s", t.conn.RemoteAddr(), t.conn.LocalAddr())
		for {
			// 设置超时时间
			//t.conn.SetDeadline(time.Now().Add(t.timeout))
			// Read a message from the message broker
			// 这里会阻塞
			// Print the message
			buf := make([]byte, 1024*4*10)
			n, err := t.conn.Read(buf)
			if err != nil {
				if err == io.EOF {
					for {
						fmt.Println("Connection closed:", err)
						fmt.Println("Connection retry:")
						if err = t.Start(); err != nil {
							log.Printf("Connection retry: error %+v", err)
						} else {
							break
						}
						time.Sleep(time.Second * 3)
					}
				}
			} else {
				remotingCommandResp := protocol.RemotingCommand{}
				if err = proto.Unmarshal(buf[:n], &remotingCommandResp); err != nil {
					log.Printf("remotingCommandResp error: %v", err)
				} else {
					if remotingCommandResp.Header.Code != 200 {
						log.Printf("请求返回异常：%+v", remotingCommandResp)
						log.Printf("请求返回异常：%s", string(remotingCommandResp.Body))
					} else {
						switch remotingCommandResp.Header.Opaque {
						case protocol.OpaqueType_Subscription:
							//log.Printf("收到订阅消息的响应")
							responseData := protocol.SubscriptionResponseData{}
							if err = proto.Unmarshal(remotingCommandResp.Body, &responseData); err != nil {
								log.Printf("收取订阅消息异常 error: %v", err)
							} else {
								//log.Printf("收取订阅消息内容: %+v", responseData)
								if responseData.ConsumeProgress != nil && len(responseData.ConsumeProgress) > 0 {
									t.queues = responseData.ConsumeProgress
								}
								// 开启同步
								go t.SendSyncTopicInfo()
								go t.SendHeartbeat()
							}
						case protocol.OpaqueType_SyncTopicRouteInfo:
							go t.SyncTopicInfoHandler(remotingCommandResp.Body)
						case protocol.OpaqueType_Unsubscription:
						case protocol.OpaqueType_Publish:
						case protocol.OpaqueType_PullMessage:
							//log.Printf("收到消息返回")
							responseData := protocol.PullMessageResponseData{}
							//log.Printf("responseData md5:%d", md5.Sum(remotingCommandResp.Body))
							if err = proto.Unmarshal(remotingCommandResp.Body, &responseData); err != nil {
								log.Fatal("remotingCommandResp error: %v", err)
							} else {
								t.ProcessPullMessageHandler(responseData.Messages)
							}
						}
					}
				}
			}
		}
	}()
}

func (t *TitanConsumerClient) PullMessage() {
	for {
		// 如果还没有注册clientid， 并且获取到的队列为空，继续等待
		if t.clientId == "" || len(t.queues) < 1 || t.pullMessageLock == false {
			time.Sleep(3 * time.Second)
			continue
		}

		// 随机获取一个队列
		rand.Seed(time.Now().UnixNano())
		randQueueIndex := rand.Intn(len(t.queues))
		queue := t.queues[randQueueIndex]
		requestData := protocol.PullMessageRequestData{
			ClientId:      t.clientId,
			Topic:         t.topic,
			ConsumerGroup: t.consumerGroupName,
			PullSize:      t.pullMessageSize,
			QueueId:       queue.QueueId, // 随机获取一个队列
			Offset:        queue.Offset,
		}
		log.Printf("开始拉取数据, ClientId %s,offset:%d, queueId:%d, pullSize:%d", t.clientId, queue.Offset, queue.QueueId, t.pullMessageSize)
		requestDataBytes, _ := proto.Marshal(&requestData)
		t.SendCommand(protocol.OpaqueType_PullMessage, requestDataBytes)
		t.pullMessageLock = false

	}

}

func (t *TitanConsumerClient) ProcessPullMessageHandler(messages []*protocol.Message) {
	t.queueOffsetLock.Lock()
	defer t.queueOffsetLock.Unlock()
	for _, msg := range messages {
		log.Printf("收到消息: msgId:%+v, QueueId:%d, QueueOffset:%d", msg.MsgId, msg.QueueId, msg.QueueOffset)
		// 消费完之后，更新当前的消费offset
		for _, q := range t.queues {
			if q.QueueId == msg.QueueId && msg.QueueOffset >= q.Offset {
				q.Offset = msg.QueueOffset + 1
			}
		}
	}
	t.pullMessageLock = true
}

// 每5秒同步一次信息
func (t *TitanConsumerClient) SendSyncTopicInfo() {
	for {
		log.Printf("发送同步主题消息的请求")
		req := &protocol.SyncTopicRouteRequestData{
			Topic:         t.topic,
			ConsumerGroup: t.consumerGroupName,
			ClientId:      t.clientId,
		}
		bytes, _ := proto.Marshal(req)
		t.SendCommand(protocol.OpaqueType_SyncTopicRouteInfo, bytes)
		time.Sleep(5 * time.Second)
	}
}

// 每5秒同步一次信息
func (t *TitanConsumerClient) SendHeartbeat() {
	for {
		if t.clientId != "" {
			//var consumeProgress = make([]*protocol.ConsumeProgress, 0)
			//for queueId, offset := range t.queueOffset {
			//	consumeProgress = append(consumeProgress, &protocol.ConsumeProgress{
			//		QueueId: queueId, // 由于0值会丢失，所以模式加一
			//		Offset:  offset,
			//	})
			//}
			req := &protocol.HeartbeatRequestData{
				Topic:           t.topic,
				ConsumerGroup:   t.consumerGroupName,
				ClientId:        t.clientId,
				ConsumeProgress: t.queues,
			}
			log.Printf("发送心跳请求 %+v", req)
			bytes, _ := proto.Marshal(req)
			t.SendCommand(protocol.OpaqueType_Heartbeat, bytes)
		}
		time.Sleep(5 * time.Second)
	}
}

func (t *TitanConsumerClient) SendCommand(opaque protocol.OpaqueType, dataBytes []byte) {
	remotingCommandReq := protocol.RemotingCommand{
		Type: protocol.RemotingCommandType_RequestCommand,
		Header: &protocol.RemotingCommandHeader{
			Code:   0,
			Opaque: opaque,
			Flag:   0,
			Remark: "",
		},
		Body: dataBytes,
	}
	remotingCommandBytes, _ := proto.Marshal(&remotingCommandReq)

	// 发送请求
	_, err := t.conn.Write(remotingCommandBytes)
	if err != nil {
		log.Printf("发送请求异常: %v", err)
		return
	}
}

func (t *TitanConsumerClient) SyncTopicInfoHandler(body []byte) {
	var resp protocol.SyncTopicRouteResponseData
	if err := proto.Unmarshal(body, &resp); err != nil {
		log.Printf("SyncTopicInfoHandler error: %v", err)
		return
	}
	log.Printf("收到同步主题的消息, 队列：%+v", resp.ConsumeProgress)
	t.queueOffsetLock.Lock()
	defer t.queueOffsetLock.Unlock()
	newQueue := make(map[int32]*protocol.ConsumeProgress)
	// 这里需要交叉对比, 因为队列可能会增减，先与同步过来的信息对其
	for _, q := range resp.ConsumeProgress {
		newQueue[q.QueueId] = q
	}

	for _, q := range t.queues {
		if nq, ok := newQueue[q.QueueId]; !ok {
			continue
		} else {
			if nq.Offset > q.Offset {
				q.Offset = nq.Offset // 取最大的offset
			}
		}
	}
	result := make([]*protocol.ConsumeProgress, 0)
	for _, q := range newQueue {
		result = append(result, q)
	}
	t.queues = result
}

func GenerateSerialNumber(prefix string) string {
	// 获取当前日期时间，格式为 YYYYMMDDHHMMSS
	now := time.Now().Format("20060102150405")

	// 可选的其他序列号部分
	// 可以根据需要添加其他部分，例如随机数
	// 这些部分可以用分隔符分隔，以形成一个完整的序列号
	// 例如：20190314123145234-2345
	rand.Seed(time.Now().UnixNano())
	randNum := rand.Intn(100000)
	randNumStr := fmt.Sprintf("%05d", randNum)

	// 组合序列号
	serialNumber := fmt.Sprintf("%s%s-%s", prefix, now, randNumStr)

	return serialNumber
}
