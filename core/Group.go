package core

type ConsumerGroup struct {
	TopicName string
	GroupName string
	Clients   map[string]*Client
}

type ProducerGroup struct {
	TopicName string
	GroupName string
	Clients   []*Client
}
