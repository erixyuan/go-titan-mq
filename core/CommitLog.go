package core

import (
	"encoding/binary"
	"fmt"
	"os"
)

type CommitLog struct {
	path        string
	file        *os.File
	currentPos  int64 // 当前写入的位置
	maxFileSize int64 // 单个文件最大大小
}

const (
	MessageMagicCode = 0xAABBCCDD // 消息魔数，用于校验
	MessageHeaderLen = 20         // 消息头部长度，包括消息魔数、消息长度、消息队列ID、消息标志位、消息物理偏移量
)

// 消息头部结构体
type MessageHeader struct {
	MagicCode       int32 // 魔数，用于校验
	BodyLen         int32 // 消息体长度
	QueueId         int32 // 消息队列ID
	Flag            int32 // 消息标志位
	CommitLogOffset int64 // 消息物理偏移量
}

// 创建CommitLog实例
func NewCommitLog(path string, maxFileSize int64) (*CommitLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &CommitLog{
		path:        path,
		file:        f,
		currentPos:  stat.Size(),
		maxFileSize: maxFileSize,
	}, nil
}

// 消息写入
func (c *CommitLog) Append(msg []byte, queueId int32, flag int32) (int64, error) {
	// 消息头部
	header := &MessageHeader{
		MagicCode:       MessageMagicCode,
		BodyLen:         int32(len(msg)),
		QueueId:         queueId,
		Flag:            flag,
		CommitLogOffset: c.currentPos,
	}
	headerBytes := make([]byte, MessageHeaderLen)
	binary.BigEndian.PutUint32(headerBytes[0:4], uint32(header.MagicCode))
	binary.BigEndian.PutUint32(headerBytes[4:8], uint32(header.BodyLen))
	binary.BigEndian.PutUint32(headerBytes[8:12], uint32(header.QueueId))
	binary.BigEndian.PutUint32(headerBytes[12:16], uint32(header.Flag))
	binary.BigEndian.PutUint64(headerBytes[16:24], uint64(header.CommitLogOffset))
	// 写入消息头部和消息体
	n, err := c.file.Write(append(headerBytes, msg...))
	if err != nil {
		return 0, err
	}
	if n != MessageHeaderLen+len(msg) {
		return 0, fmt.Errorf("write message len not match, expect %d, actual %d", MessageHeaderLen+len(msg), n)
	}
	// 更新当前写入的位置
	c.currentPos += int64(n)
	// 如果文件大小超过了最大值，就创建新的文件
	if c.currentPos >= c.maxFileSize {
		if err := c.file.Close(); err != nil {
			return 0, err
		}
		newPath := fmt.Sprintf("%s.%d", c.path, c.currentPos/c.maxFileSize)
		f, err := os.OpenFile(newPath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return 0, err
		}
		c.file = f
		c.currentPos = 0
	}
	return header.CommitLogOffset, nil
}

// 根据offset读取多条消息
func (c *CommitLog) GetMessage(offset int64, num int) ([][]byte, error) {
	result := [][]byte{}
	for i := 0; i < num; i++ {
		// 读取消息头部
		headerBytes := make([]byte, MessageHeaderLen)
		if _, err := c.file.ReadAt(headerBytes, offset); err != nil {
			return nil, err
		}
		header := &MessageHeader{
			MagicCode:       int32(binary.BigEndian.Uint32(headerBytes[0:4])),
			BodyLen:         int32(binary.BigEndian.Uint32(headerBytes[4:8])),
			QueueId:         int32(binary.BigEndian.Uint32(headerBytes[8:12])),
			Flag:            int32(binary.BigEndian.Uint32(headerBytes[12:16])),
			CommitLogOffset: int64(binary.BigEndian.Uint64(headerBytes[16:24])),
		}
		if header.MagicCode != MessageMagicCode {
			return nil, fmt.Errorf("magic code not match, expect %d, actual %d", MessageMagicCode, header.MagicCode)
		}
		// 读取消息体
		msgBytes := make([]byte, header.BodyLen)
		if _, err := c.file.ReadAt(msgBytes, offset+MessageHeaderLen); err != nil {
			return nil, err
		}
		// 将消息体加入结果列表
		result = append(result, msgBytes)
		// 更新偏移量
		offset += int64(MessageHeaderLen + header.BodyLen)
	}
	return result, nil
}
