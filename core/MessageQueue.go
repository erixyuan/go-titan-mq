package core

import "sync"

type MessageQueue struct {
	messages []*Message // 消息队列
	mutex    sync.Mutex // 互斥锁
}

// Push 将消息加入队列
func (mq *MessageQueue) Push(msg *Message) {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	mq.messages = append(mq.messages, msg)
}

// Pop 从队列中取出消息
func (mq *MessageQueue) Pop() *Message {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()

	if len(mq.messages) == 0 {
		return nil
	}

	msg := mq.messages[0]
	mq.messages = mq.messages[1:]

	return msg
}
