package core

import (
	"github.com/erixyuan/go-titan-mq/protocol"
	"log"
)

const (
	QueueNumsDefault int = 1
)

type Topic struct {
	topicName      string
	consumerGroups map[string]*ConsumerGroup
	producerGroups map[string]*ProducerGroup
	ConsumeQueues  []*ConsumeQueue
}

func NewTopic(topicName string) *Topic {
	log.Printf("开始初始化主题[%s]......", topicName)
	m1 := make(map[string]*ConsumerGroup)
	m2 := make(map[string]*ProducerGroup)
	q := make([]chan protocol.Message, 0)
	q = append(q, make(chan protocol.Message, 10))
	tp := Topic{
		topicName:      topicName,
		consumerGroups: m1,
		producerGroups: m2,
	}
	for i := 0; i < QueueNumsDefault; i++ {
		queue, err := NewConsumeQueue(topicName, i)
		if err != nil {
			log.Fatalf("创建consume queue error: %v", err)
		}
		tp.ConsumeQueues = append(tp.ConsumeQueues, queue)
	}

	return &tp
}
