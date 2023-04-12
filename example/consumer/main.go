package main

import (
	"github.com/erixyuan/go-titan-mq/broker"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/erixyuan/go-titan-mq/sdk"
	"github.com/sirupsen/logrus"
	"log"
	"os"
)

var Log = logrus.New()

func main() {

	// 设置日志级别为 Debug，并将日志输出到标准输出
	Log.SetLevel(logrus.InfoLevel)
	Log.SetOutput(os.Stdout)
	Log.SetReportCaller(true)

	go func() {
		var messageNums int
		topic := "news"
		consumerGroupName := "GID-C-01"
		titanClient := sdk.TitanConsumerClient{}
		titanClient.Init("localhost:9999", topic, consumerGroupName, func(message *protocol.Message) error {
			messageNums++
			Log.Infof("收到%d个消息", messageNums)
			return nil
		})
		if err := titanClient.Start(); err != nil {
			Log.Fatalf("连接服务器异常 error %+v", err)
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
