package core

import (
	"fmt"
	"os"
)

const (
	topic             = "test_topic"
	queueNums         = 4
	indexNums         = 1000000
	consumeQueueDir   = "./consumequeue"
	consumeQueueSize  = 20 * 1024 * 1024
	consumeQueueExt   = "cq"
	indexFileExt      = "idx"
	messageMagicCode  = 0xAABBCCDD
	messageFlag       = 0x0
	messageProperties = 0
	messageBodyCRC    = 0
)

type ConsumeQueue struct {
	topic            string
	queueNums        int
	storePathRootDir string
	dataFileList     []*os.File
}

func NewConsumeQueue(topic string, queueNums int, storePathRootDir string) (*ConsumeQueue, error) {
	cq := &ConsumeQueue{
		topic:            topic,
		queueNums:        queueNums,
		storePathRootDir: storePathRootDir,
		dataFileList:     make([]*os.File, queueNums),
	}
	if err := cq.init(); err != nil {
		return nil, err
	}
	return cq, nil
}
func (cq *ConsumeQueue) init() error {
	for i := 0; i < cq.queueNums; i++ {
		dirPath := fmt.Sprintf("%s/%s/%d", cq.storePathRootDir, cq.topic, i)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return err
		}
		file, err := os.OpenFile(fmt.Sprintf("%s/%s/%d/%s.%s", cq.storePathRootDir, cq.topic, i, "consumequeue", consumeQueueExt), os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		cq.dataFileList[i] = file
	}
	return nil
}
func (cq *ConsumeQueue) putMessage(queueId int, offset int64, size int32) error {
	file, err := cq.dataFileList[queueId]
	if err != nil {
		return err
	}
	if _, err := file.Seek(offset, 0); err != nil {
		return err
	}
	if _, err := file.Write([]byte{byte(size >> 24), byte(size >> 16), byte(size >> 8), byte(size)}); err != nil {
		return err
	}
	if _, err := file.Write([]byte{byte(offset >> 56), byte(offset >> 48), byte(offset >> 40), byte(offset >> 32), byte(offset >> 24), byte(offset >> 16), byte(offset >> 8), byte(offset)}); err != nil {
		return err
	}
	return nil
}
func (cq *ConsumeQueue) getMessage(queueId int, offset int64) (int64, int32, error) {
	file, err := cq.dataFileList[queueId]
	if err != nil {
		return 0, 0, err
	}
	if _, err := file.Seek(offset, 0); err != nil {
		return 0, 0, err
	}
	sizeBytes := make([]byte, 4)
	if _, err := file.Read(sizeBytes); err != nil {
		return 0, 0, err
	}
	size := int32(sizeBytes[0])<<24 | int32(sizeBytes[1])<<16 | int32(sizeBytes[2])<<8 | int32(sizeBytes[3])
	offsetBytes := make([]byte, 8)
	if _, err := file.Read(offsetBytes); err != nil {
		return 0, 0, err
	}
	offset = int64(offsetBytes[0])<<56 | int64(offsetBytes[1])<<48 | int64(offsetBytes[2])<<40 | int64(offsetBytes[3])<<32 | int64(offsetBytes[4])<<24 | int64(offsetBytes[5])<<16 | int64(offsetBytes[6])<<8 | int64(offsetBytes[7])
	return offset, size, nil
}
func main() {
	cq, err := NewConsumeQueue(topic, queueNums, consumeQueueDir)
	if err != nil {
		panic(err)
	}
	// put message
	if err := cq.putMessage(0, 0, 100); err != nil {
		panic(err)
	}
	if err := cq.putMessage(0, 100, 200); err != nil {
		panic(err)
	}
	if err := cq.putMessage(1, 0, 300); err != nil {
		panic(err)
	}
	if err := cq.putMessage(2, 0, 400); err != nil {
		panic(err)
	}
	// get message
	offset, size, err := cq.getMessage(0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("offset: %d, size: %d\n", offset, size)
	offset, size, err = cq.getMessage(0, 4)
	if err != nil {
		panic(err)
	}
	fmt.Printf("offset: %d, size: %d\n", offset, size)
	offset, size, err = cq.getMessage(1, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("offset: %d, size: %d\n", offset, size)
	offset, size, err = cq.getMessage(2, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("offset: %d, size: %d\n", offset, size)
}
