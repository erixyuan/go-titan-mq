package core

import "github.com/erixyuan/go-titan-mq/protocol"

type Topic struct {
	topicName      string
	consumerGroups map[string]*ConsumerGroup
	producerGroups map[string]*ProducerGroup
	queues         []chan protocol.Message
}
