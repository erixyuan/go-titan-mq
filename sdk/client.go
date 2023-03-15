package sdk

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/core"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

var ok = "ok"
var subscribe = "subscribe"

type GoTitanClient struct {
	address      string
	timeout      time.Duration
	retryTime    time.Duration
	conn         net.Conn
	callback     map[string]func(core.Message)
	acceptIsOpen bool
}

// 创建客户端
func (t *GoTitanClient) Init(address string) {
	t.address = address
	t.timeout = 1 * time.Second
	t.retryTime = 1 * time.Second
	t.callback = make(map[string]func(core.Message))
	t.acceptIsOpen = false

}

// 设置超时时间
func (t *GoTitanClient) SetTimeout(timeout int) {
	t.timeout = time.Duration(timeout) * time.Second
}

func (t *GoTitanClient) Connect() error {
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
func (t *GoTitanClient) Subscribe(topic string, f func(core.Message)) error {
	t.send(topic, subscribe)
	t.callback[topic] = f
	return nil
}

func (t *GoTitanClient) Public(topic string, content string) error {
	return t.send(topic, content)
}

func (t *GoTitanClient) send(topic string, payload string) error {
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

// 等待数据过来
func (t *GoTitanClient) accept() {
	go func() {
		//// Continuously listen for messages from the message broker
		log.Printf("开始等待消息返回 %s %s", t.conn.RemoteAddr(), t.conn.LocalAddr())
		for {
			// 设置超时时间
			t.conn.SetDeadline(time.Now().Add(t.timeout))
			reader := bufio.NewReader(t.conn)

			// Read a message from the message broker
			// 这里会阻塞
			msg, err := reader.ReadString('\n')
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
						time.Sleep(10 * time.Second)
					}
				}
			} else {
				// Print the message
				msg = strings.TrimSuffix(msg, "\n")
				fmt.Printf("Received message %s\n", msg)
			}
		}
	}()

}
