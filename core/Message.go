package core

import "encoding/json"

type Message struct {
	MessageNo string
	Topic     string
	Payload   string
}

// Marshal 将消息转换为json格式的字节数组
func (msg *Message) Marshal() ([]byte, error) {
	return json.Marshal(msg)
}

// Unmarshal 将json格式的字节数组转换为消息
func (msg *Message) Unmarshal(data []byte) error {
	return json.Unmarshal(data, msg)
}
