package main

import (
	"github.com/erixyuan/go-titan-mq/core"
	"log"
)

func main() {
	broker := core.NewBroker()
	if err := broker.Start(9999); err != nil {
		log.Fatalf("启动服务失败")
	}
}
