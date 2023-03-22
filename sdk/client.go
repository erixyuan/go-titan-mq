package sdk

import (
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/core"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
	"time"
)

var ok = "ok"
var subscribe = "subscribe"

type TitanConsumerClient struct {
	address           string
	timeout           time.Duration
	retryTime         time.Duration
	conn              net.Conn
	callback          map[string]func(core.Message)
	acceptIsOpen      bool
	clientId          string
	consumerGroupName string
	topic             string
}

// 创建客户端
func (t *TitanConsumerClient) Init(address string, topic string, name string) {
	t.address = address
	t.timeout = 1 * time.Second
	t.retryTime = 1 * time.Second
	t.callback = make(map[string]func(core.Message))
	t.acceptIsOpen = false
	t.topic = topic
	t.clientId = GenerateSerialNumber("CLIENT")
	t.consumerGroupName = name
}

// 设置超时时间
func (t *TitanConsumerClient) SetTimeout(timeout int) {
	t.timeout = time.Duration(timeout) * time.Second
}

func (t *TitanConsumerClient) Connect() error {
	log.Printf("开始连接地址：%s", t.address)
	conn, err := net.DialTimeout("tcp", t.address, t.timeout)
	if err != nil {
		return err
	}

	// 设置读写的超时时间
	if err = conn.SetDeadline(time.Now().Add(t.timeout)); err != nil {
		return err
	}
	t.conn = conn

	// 开协程进行循环读
	if t.acceptIsOpen == false {
		t.accept()
		t.acceptIsOpen = true
	}

	return nil
}

// 订阅
func (t *TitanConsumerClient) Subscribe(topic string, f func(core.Message)) error {
	command := protocol.RemotingCommand{
		Type: protocol.RemotingCommandType_RequestCommand,
		Header: &protocol.RemotingCommandHeader{
			Code:   0,
			Opaque: protocol.OpaqueType_Subscription,
			Flag:   0,
			Remark: "",
		},
	}
	body := protocol.SubscriptionRequestData{
		Topic:         t.topic,
		ConsumerGroup: t.consumerGroupName,
		ClientId:      t.clientId,
	}
	if bodyBytes, err := proto.Marshal(&body); err != nil {
		log.Printf("Subscribe error: %v", err)
	} else {
		command.Body = bodyBytes
		if commandBytes, err := proto.Marshal(&command); err != nil {
			log.Printf("Subscribe error: %v", err)
		} else {
			_, err := t.conn.Write(commandBytes)
			if err != nil {
				fmt.Printf("Failed to send request: %v\n", err)
			}
		}
	}
	log.Printf("发送订阅消息成功，等待回复")
	return nil
}

func (t *TitanConsumerClient) Public(topic string, content string) error {
	return t.send(topic, content)
}

func (t *TitanConsumerClient) send(topic string, payload string) error {
	message := core.Message{
		Topic:   topic,
		Payload: payload,
	}
	s, _ := json.Marshal(message)
	b := string(s)
	log.Println("发送消息：", b)
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
			t.conn.SetDeadline(time.Now().Add(t.timeout))
			// Read a message from the message broker
			// 这里会阻塞
			// Print the message
			buf := make([]byte, 1024)
			n, err := t.conn.Read(buf)
			if err != nil {
				if err == io.EOF {
					for {
						fmt.Println("Connection closed:", err)
						fmt.Println("Connection retry:")
						if err = t.Connect(); err != nil {
							log.Printf("Connection retry: error %+v", err)
						} else {
							break
						}
					}
				}
			} else {
				remotingCommandResp := protocol.RemotingCommand{}
				if err = proto.Unmarshal(buf[:n], &remotingCommandResp); err != nil {
					log.Printf("remotingCommandResp error: %v", err)
				} else {
					switch remotingCommandResp.Header.Opaque {
					case protocol.OpaqueType_Subscription:
						log.Printf("收到订阅消息的响应")
						responseData := protocol.SubscriptionResponseData{}
						if err = proto.Unmarshal(remotingCommandResp.Body, &responseData); err != nil {
							log.Printf("remotingCommandResp error: %v", err)
						} else {
							if remotingCommandResp.Header.Code == 200 {
								t.PullMessage()
							}
						}
					case protocol.OpaqueType_Unsubscription:
					case protocol.OpaqueType_Publish:
					case protocol.OpaqueType_PullMessage:
						responseData := protocol.PullMessageResponseData{}
						if err = proto.Unmarshal(remotingCommandResp.Body, &responseData); err != nil {
							log.Printf("remotingCommandResp error: %v", err)
						} else {
							t.ProcessPullMessageHandler(responseData.Messages)
						}
					}

				}
			}
		}
	}()
}

func (t *TitanConsumerClient) PullMessage() {
	if t.clientId == "" {
		time.Sleep(3 * time.Second)
		if t.clientId == "" {
			log.Printf("client Id 不存在 error")
			return
		}
	}
	log.Printf("开始拉取数据, ClientId %s", t.clientId)
	requestData := protocol.PullMessageRequestData{
		ClientId:      t.clientId,
		Topic:         t.topic,
		ConsumerGroup: t.consumerGroupName,
		PullSize:      5,
	}
	requestDataBytes, _ := proto.Marshal(&requestData)

	remotingCommandReq := protocol.RemotingCommand{
		Type: protocol.RemotingCommandType_RequestCommand,
		Header: &protocol.RemotingCommandHeader{
			Code:   0,
			Opaque: protocol.OpaqueType_PullMessage,
			Flag:   0,
			Remark: "",
		},
		Body: requestDataBytes,
	}
	remotingCommandBytes, _ := proto.Marshal(&remotingCommandReq)

	// 发送请求
	_, err := t.conn.Write(remotingCommandBytes)
	if err != nil {
		fmt.Printf("Failed to send request: %v\n", err)
		return
	}
}

func (t *TitanConsumerClient) ProcessPullMessageHandler(messages []*protocol.Message) {
	for _, msg := range messages {
		log.Printf("收到消息: %+v", msg)
	}
	t.PullMessage()
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
	serialNumber := fmt.Sprintf("%s-%s", now, randNumStr)

	return serialNumber
}
