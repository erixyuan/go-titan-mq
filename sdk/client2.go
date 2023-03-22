package sdk

//
//import "context"
//
//func main() {
//	consumer := rocketmq.NewDefaultConsumer("consumer_group")
//	consumer.SetNameServerAddress("localhost:9876")
//	consumer.Subscribe("test_topic", "test_tag")
//	consumer.RegisterMessageListener(func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
//		// 处理消息
//		return consumer.ConsumeSuccess, nil
//	})
//	consumer.Start()
//
//}
