package main

import (
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"net"
	"time"
)

func main() {
	// 建立 TCP 连接
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		return
	}
	defer conn.Close()

	for {

		// 创建一个Message对象
		msg := &protocol.Message{
			Topic:          "test",
			Body:           []byte(""),
			BornTimestamp:  1234567890,
			StoreTimestamp: 1234567890,
			MsgId:          "1234567890",
			ProducerGroup:  "producer",
			ConsumerGroup:  "consumer",
		}

		// 将Message对象编码为二进制数据
		data, err := proto.Marshal(msg)

		// 发送请求
		numRequested := 5
		_, err = conn.Write(data)
		if err != nil {
			fmt.Printf("Failed to send request: %v\n", err)
			return
		}

		// 接收响应
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("Failed to receive response: %v\n", err)
			return
		}

		fmt.Printf("Received %d messages:\n%s\n", numRequested, string(buf[:n]))
		time.Sleep(time.Second * 3)
	}
}
