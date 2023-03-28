package broker

import (
	"github.com/erixyuan/go-titan-mq/protocol"
)

type MessageQueue struct {
	TopicName      string
	QueueId        string
	MessageChannel chan protocol.Message
}

func NewMessageQueue(topic string, id int) *MessageQueue {
	return &MessageQueue{
		TopicName:      "",
		QueueId:        "",
		MessageChannel: make(chan protocol.Message, 100),
	}
}
