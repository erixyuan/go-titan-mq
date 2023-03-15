package main

import (
	"github.com/erixyuan/go-titan-mq/core"
	"github.com/erixyuan/go-titan-mq/sdk"
	"log"
)

func main() {

	//// Connect to the message broker
	//conn, err := net.Dial("tcp", "localhost:9999")
	//if err != nil {
	//	fmt.Println("Error connecting to message broker:", err.Error())
	//	os.Exit(1)
	//}
	//
	//time.Sleep(time.Second)
	//
	//// Subscribe to a topic
	//topic := "news"
	//message := core.Message{
	//	Topic:   topic,
	//	Payload: "subscribe",
	//}
	//s, _ := json.Marshal(message)
	//b := string(s)
	//fmt.Fprintln(conn, b)
	//
	//// Continuously listen for messages from the message broker
	//reader := bufio.NewReader(conn)
	//for {
	//	log.Printf("开始等待 %s %s", conn.RemoteAddr(), conn.LocalAddr())
	//	// Read a message from the message broker
	//	msg, err := reader.ReadString('\n')
	//	if err != nil {
	//		fmt.Println("Error reading message from message broker:", err.Error())
	//		os.Exit(1)
	//	}
	//
	//	// Print the message
	//	msg = strings.TrimSuffix(msg, "\n")
	//	fmt.Printf("Received message on topic '%s': %s\n", topic, msg)
	//}

	titanClient := sdk.GoTitanClient{}
	titanClient.Init("localhost:9999")
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
