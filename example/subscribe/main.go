package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/core"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func main() {

	// Connect to the message broker
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		fmt.Println("Error connecting to message broker:", err.Error())
		os.Exit(1)
	}

	time.Sleep(time.Second)

	// Subscribe to a topic
	topic := "news"
	message := core.Message{
		Topic:   topic,
		Payload: "subscribe",
	}
	s, _ := json.Marshal(message)
	b := string(s)
	fmt.Fprintln(conn, b)

	// Continuously listen for messages from the message broker
	reader := bufio.NewReader(conn)
	for {
		log.Printf("开始等待 %s %s", conn.RemoteAddr(), conn.LocalAddr())
		// Read a message from the message broker
		msg, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading message from message broker:", err.Error())
			os.Exit(1)
		}

		// Print the message
		msg = strings.TrimSuffix(msg, "\n")
		fmt.Printf("Received message on topic '%s': %s\n", topic, msg)
	}
}
