package main

import (
	"github.com/erixyuan/go-titan-mq/broker"
	"github.com/erixyuan/go-titan-mq/sdk"
	"log"
)

func main() {
	go func() {
		topic := "news"
		consumerGroupName := "GID-C-01"
		titanClient := sdk.TitanConsumerClient{}
		titanClient.Init("localhost:9999", topic, consumerGroupName)
		if err := titanClient.Start(); err != nil {
			log.Printf("连接服务器异常 error %+v", err)
			return
		}
	}()
	//go func() {
	//	topic := "news"
	//	consumerGroupName := "GID-C-02"
	//	titanClient := sdk.TitanConsumerClient{}
	//	titanClient.Init("localhost:9999", topic, consumerGroupName)
	//	if err := titanClient.Start(); err != nil {
	//		log.Printf("连接服务器异常 error %+v", err)
	//		return
	//	}
	//}()

	for {

	}
}

func consume(msg broker.Message) {
	log.Println(msg)
}
