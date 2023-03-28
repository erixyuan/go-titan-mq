package core

type ProducerGroup struct {
	TopicName string
	GroupName string
	Clients   []*Client
}
