package main

import (
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"
)

func handleConn(conn net.Conn, ch chan string, reqMsg *protocol.Message) {
	log.Println("开始处理conn")
	var messages []string
	//numRequested := int(reqMsg.PullSize)
	numRequested := 4
	for i := 0; i < numRequested; i++ {
		select {
		case message := <-ch:
			log.Println("收到消息:", message)
			messages = append(messages, message)
		case <-time.After(10 * time.Second):
			fmt.Println("Timeout, returning partial response.")
			_, err := conn.Write([]byte(strings.Join(messages, "\n")))
			if err != nil {
				fmt.Printf("Failed to send response: %v\n", err)
			}
			return
		}
	}

	fmt.Printf("Returning %d messages to client.\n", numRequested)
	_, err := conn.Write([]byte(strings.Join(messages, "\n")))
	if err != nil {
		fmt.Printf("Failed to send response: %v\n", err)
	}
}
func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Failed to listen on port 8080: %v\n", err)
		return
	}
	defer ln.Close()

	fmt.Println("Server started, waiting for connections...")

	queues := make(map[int]chan string) // 创建一个 map，用于存储每个客户端连接的队列

	// 定时器，每秒钟向一个随机的队列中生产一个消息
	go func() {
		nums := 1
		for {
			time.Sleep(1 * time.Second)
			rand.Seed(time.Now().UnixNano())
			log.Println(len(queues))
			if len(queues) > 0 {
				i := rand.Intn(len(queues))
				q := queues[i]
				msg := fmt.Sprintf("Message%d from timer, sent to queue %d", nums, i)
				log.Println(msg)
				nums += 1
				if q != nil {
					q <- msg
					log.Println("发送消息成功")
				}
			}
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		fmt.Printf("Client %s connected.\n", conn.RemoteAddr().String())

		// 创建一个带缓冲的 channel，并将其加入到队列中
		q := make(chan string, 10)
		queues[len(queues)] = q

		// 启动一个新的 goroutine 处理该连接
		go func(q chan string) {
			for {
				// 发送请求，告诉 handleConn 函数需要获取多少条消息
				buf := make([]byte, 1024)
				n, err := conn.Read(buf)
				if err != nil {
					fmt.Printf("Failed to read request from client: %v\n", err)
					return
				} else {
					log.Println("收到客户端数据：", string(buf))
				}

				msg := &protocol.Message{}
				err = proto.Unmarshal(buf[:n], msg)
				if err != nil {
					log.Fatal("unmarshaling error: ", err)
				}
				// 处理连接
				handleConn(conn, q, msg)
			}
		}(q)
	}
}
