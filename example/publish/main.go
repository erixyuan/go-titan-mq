package main

import (
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/core"
	"net"
	"os"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		fmt.Println("Error connecting:", err)
		os.Exit(1)
	}
	defer conn.Close()

	count := 1
	for {
		// 发送消息到 news 主题
		message := core.Message{
			Topic:   "news",
			Payload: fmt.Sprintf("Hello,world%d!", count),
		}
		s, _ := json.Marshal(message)
		b := string(s)
		// 发送带有结尾符号的的消息
		fmt.Fprintln(conn, b)
		count += 1
		time.Sleep(time.Second)
	}

}
