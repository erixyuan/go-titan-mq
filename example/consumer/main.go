package main

import (
	"github.com/erixyuan/go-titan-mq/core"
	"github.com/erixyuan/go-titan-mq/sdk"
	"log"
)

func main() {
	topic := "news"
	consumerGroupName := "GID-C-01"
	titanClient := sdk.TitanConsumerClient{}
	titanClient.Init("localhost:9999", topic, consumerGroupName)
	if err := titanClient.Connect(); err != nil {
		log.Printf("连接服务器异常 error %+v", err)
		return
	}
	if err := titanClient.Subscribe("news", consume); err != nil {
		log.Printf("client.Subscribe error %+v", err)
	}
	for {

	}
}

func consume(msg core.Message) {
	log.Println(msg)
}
