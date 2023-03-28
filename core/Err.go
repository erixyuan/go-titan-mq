package core

import "errors"

var (
	ErrRequest               = errors.New("请求体错误")
	ErrMessageNotFound       = errors.New("找不到消息")
	ErrMessageNotYet         = errors.New("还没有消息")
	ErrTopicNotExist         = errors.New("主题不存在")
	ErrConsumerGroupNotExist = errors.New("消费组不存在")
	ErrQueueId               = errors.New("队列错误")
	ErrClientExist           = errors.New("客户端已经存在")
)
