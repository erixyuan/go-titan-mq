package broker

type ConsumerGroup struct {
	TopicName string             // 主题名称
	GroupName string             // 消费组名称
	Clients   map[string]*Client // string = clientId
}
