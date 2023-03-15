package core

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

/**
subscribe 命令表示客户端订阅某个主题，这里将连接 conn 记录在该主题对应的订阅者集合中。
unsubscribe 命令表示客户端取消订阅某个主题，这里将连接 conn 从该主题对应的订阅者集合中删除。
publish 命令表示客户端发布一条消息，这里会遍历该主题对应的订阅者集合，将消息发送给所有的订阅者。

在处理 publish 命令时，需要注意如果向某个订阅者发送消息失败了，需要将该订阅者从该主题对应的订阅者集合中删除。

*/

// Message represents a message in the message queue
type Message struct {
	MessageNo string
	Topic     string
	Payload   string
}

// Broker manages the message queue and handles client requests
type Broker struct {
	clients        map[net.Conn]bool
	subscribers    map[string][]net.Conn
	messages       chan Message            // 做成队列
	queue          map[string]chan Message // 根据topic 划分队列
	commitLog      map[string]*os.File
	mutex          sync.Mutex
	commitLogMutex sync.Mutex
}

// NewBroker creates a new message broker
func NewBroker() *Broker {
	return &Broker{
		clients:     make(map[net.Conn]bool),
		subscribers: make(map[string][]net.Conn),
		messages:    make(chan Message, 10),
		queue:       make(map[string]chan Message, 10),
		commitLog:   make(map[string]*os.File),
	}
}

// Start starts the broker and listens for client requests
func (b *Broker) Start(port int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Broker listening on port %d\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}
		fmt.Println("New client connected:", conn.RemoteAddr())
		b.clients[conn] = true
		go b.handleClient(conn)
	}
}

// Broadcast sends a message to all subscribers of a topic
func (b *Broker) Broadcast(msg Message) {
	// 消息落地
	b.storeMsgToCommitLog(&msg)
	// 加入队列
	b.AddQueue(msg)
}

// sendMessage sends a message to a single client
// todo 这里需要确保别人已经收到，需要有确认机制
func (b *Broker) sendMessage(conn net.Conn, msg Message) {
	s, _ := json.Marshal(msg)
	fmt.Fprintf(conn, "%s\n", s)
}

func (b *Broker) AddQueue(msg Message) {
	b.queue[msg.Topic] <- msg
}

// 独立的消息发送
func (b *Broker) PublishHandler(topic string) {
	go func() {
		log.Printf("启动topic：%s的消息发送channel", topic)
		select {
		case msg := <-b.queue[topic]:
			for _, conn := range b.subscribers[msg.Topic] {
				b.sendMessage(conn, msg)
			}
		}
	}()
}

// Subscribe subscribes a client to a topic
func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.subscribers[topic] = append(b.subscribers[topic], conn)

	// 如果topic对应的文件不存在，就新建，并且
	b.createTopicCommitLogFile(topic)
	fmt.Printf("Client %v subscribed to topic %s\n", conn.RemoteAddr(), topic)
}

// 创建commit log 的topic文件
func (b *Broker) createTopicCommitLogFile(topic string) *os.File {
	b.commitLogMutex.Lock()
	defer b.commitLogMutex.Unlock()
	filePath := fmt.Sprintf("%s.log", topic)
	_, err := os.Stat(filePath)
	var file *os.File
	if os.IsNotExist(err) {
		// If the file does not exist, create it
		file, err = os.Create(filePath)
		if err != nil {
			fmt.Println("Failed to create file:", err)
			if file, err = os.OpenFile(fmt.Sprintf("%s.log", topic), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err != nil {
				fmt.Println("Failed to get file:", err)
			} else {
				b.commitLog[topic] = file
			}
		} else {
			b.commitLog[topic] = file
		}
		return file
	} else {
		if file, err = os.OpenFile(fmt.Sprintf("%s.log", topic), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err != nil {
			fmt.Println("Failed to get file:", err)
		} else {
			b.commitLog[topic] = file
		}
		return file
	}
}

func (b *Broker) getTopicCommitLogFile(topic string) *os.File {
	if file, ok := b.commitLog[topic]; ok {
		return file
	} else {
		return b.createTopicCommitLogFile(topic)
	}
}

func (b *Broker) storeMsgToCommitLog(msg *Message) {
	msgNo := GenerateSerialNumber()
	msg.MessageNo = msgNo
	if str, err := json.Marshal(msg); err != nil {
		log.Printf("storeMsgToCommitLog json.Marshal error %+v", err)
	} else {
		if _, err := b.getTopicCommitLogFile(msg.Topic).WriteString(string(str) + "\n"); err != nil {
			log.Printf("storeMsgToCommitLog WriteString error %+v, str is %s", err, str)
		} else {
			log.Printf("storeMsgToCommitLog success %s", str)
			if err := b.getTopicCommitLogFile(msg.Topic).Sync(); err != nil {
				log.Printf("storeMsgToCommitLog Sync error %+v", err)
			}
		}
	}
}

// Unsubscribe unsubscribes a client from a topic
func (b *Broker) Unsubscribe(topic string, conn net.Conn) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	subscribers := b.subscribers[topic]
	for i, sub := range subscribers {
		if sub == conn {
			b.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
			fmt.Printf("Client %v unsubscribed from topic %s\n", conn.RemoteAddr(), topic)
			return
		}
	}
}

func (b *Broker) handleClient(conn net.Conn) {
	b.mutex.Lock()
	b.clients[conn] = true
	b.mutex.Unlock()

	defer func() {
		b.mutex.Lock()
		delete(b.clients, conn)
		for topic := range b.subscribers {
			for i, c := range b.subscribers[topic] {
				if c == conn {
					b.subscribers[topic] = append(b.subscribers[topic][:i], b.subscribers[topic][i+1:]...)
					break
				}
			}
		}
		conn.Close()
		b.mutex.Unlock()
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		msg, err := parseLine(line)
		if err != nil {
			fmt.Println("Error parsing message:", err)
			continue
		}
		switch msg.Payload {
		case "subscribe":
			b.Subscribe(msg.Topic, conn)
		case "unsubscribe":
			b.Unsubscribe(msg.Topic, conn)
		default:
			b.Broadcast(msg)
		}
	}
}

// parseLine parses a single line of text into a Message struct
func parseLine(line string) (Message, error) {
	fmt.Printf("接收到消息: %+v\n", line)
	var msg Message
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		return msg, fmt.Errorf("could not parse line %s", line)
	} else {
		fmt.Printf("解析成功 %+v\n", msg)
	}
	return msg, nil
}
