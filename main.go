package main

import (
	"fmt"
	"github.com/erixyuan/go-titan-mq/broker"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/erixyuan/go-titan-mq/tools"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	broker := broker.NewBroker()

	go func() {
		if false {
			time.Sleep(5 * time.Second)
			for i := 0; i < 100; i++ {
				rand.Seed(time.Now().UnixNano())
				intn := rand.Intn(2 * 4 * 1024)
				body := make([]byte, intn)
				body = append(body, []byte(fmt.Sprintf("123-%d", i))...)
				message := protocol.Message{
					Topic:          "news",
					Body:           body,
					BornTimestamp:  12312312,
					StoreTimestamp: 0,
					MsgId:          tools.GenerateSerialNumber("P") + "--" + strconv.Itoa(i),
					ProducerGroup:  "123",
					ConsumerGroup:  "123",
				}
				log.Printf("开始写入文件%d----------------------------", i)
				// 写文件
				broker.ProducerMessage(&message)
				time.Sleep(2 * time.Second)
			}
		}

	}()

	if err := broker.Start(9999); err != nil {
		log.Fatalf("启动服务失败")
	}

}
