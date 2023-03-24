package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/golang/protobuf/proto"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	// CommitLog文件大小
	CommitLogFileSize = 1024 * 1024 * 1024
	// CommitLog区块大小
	CommitLogBlockSize = 4 * 1024
	// 文件名长度
	fileNameLength = 20
	// MagicNumber 消息开始的标识
	MagicNumber uint32 = 0xdabbcf1c
	// 每个 CommitLog 文件的最大大小
	commitLogDir = "./commit_log"
)

type CommitLog struct {
	currentFilePath   string   // 当前文件名， 初始化的时候需要更新到
	currentFile       *os.File // 文件句柄
	currentFileOffset int64    // 当前文件偏移量
	writeLock         sync.Mutex
	currentOffset     int64
}

func NewCommitLog() (*CommitLog, error) {
	var file *os.File
	var filePath string
	var err error
	var max int64
	// 判断文件目录是否存在，如果不存在新建
	if _, err = os.Stat(commitLogDir); os.IsNotExist(err) {
		if err := os.MkdirAll(commitLogDir, 0755); err != nil {
			return nil, err
		}
		filePath = fmt.Sprintf("%s/%020d", commitLogDir, 0)
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
	} else {
		var files []os.FileInfo
		if files, err = ioutil.ReadDir(commitLogDir); err != nil {
			return nil, err
		} else {
			var targetFile os.FileInfo
			for _, f := range files {
				if !f.IsDir() {
					name := f.Name()
					var num int64
					num, err = strconv.ParseInt(name, 10, 64)
					if err != nil {
						log.Fatal(err)
					} else {
						if num >= max {
							max = num
							targetFile = f
						}
					}
				}
			}
			if targetFile == nil {
				log.Fatal("找不到commitLog目标文件")
			} else {
				filePath = fmt.Sprintf("%s/%s", commitLogDir, targetFile.Name())
				file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	stat, _ := file.Stat()
	return &CommitLog{
		currentFilePath:   filePath,
		currentFile:       file,
		currentFileOffset: stat.Size(),
		writeLock:         sync.Mutex{},
	}, nil
}

// WriteMessage 将Message写入CommitLog
func (cl *CommitLog) WriteMessage(msg *protocol.Message) (int64, error) {
	cl.writeLock.Lock()
	defer cl.writeLock.Unlock()
	var (
		err        error
		bodyBytes  []byte
		bodyLen    int32
		topicBytes []byte
		topicLen   int8
		totalSize  int32
		crc        int32
	)

	// 先全部写入一个buf中，计算出总长度
	// 计算消息长度
	bodyBytes, err = proto.Marshal(msg)
	bodyLen = int32(len(bodyBytes))

	//bodyBytesBuffer := bytes.NewBuffer(bodyBytes)
	//err = binary.Write(bodyBytesBuffer, binary.BigEndian, msg)
	//if err != nil {
	//	return 0, err
	//}
	//bodyLen = int32(bodyBytesBuffer.Len())

	topicBytes = []byte(msg.Topic)
	topicLen = int8(len(topicBytes))
	// 计算消息校验码
	crc = 1 // 简化处理，直接将校验码置为0
	// 构造消息体

	// 计算消息总长度
	// totalSize + magicNumber + crc + queueId + queueOffset + commitLogOffset + bornTImeStamp + body + topic
	// 4         +    4        +  4  +  4      +  8          +      8          +    8          +  (4+bodyLength) + (1+topicLength)
	totalSize = int32(4 + 4 + 4 + 4 + 8 + 8 + 8) //40
	totalSize += 4 + bodyLen
	totalSize += 1 + int32(topicLen)

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, totalSize)
	binary.Write(&buf, binary.BigEndian, MagicNumber)
	binary.Write(&buf, binary.BigEndian, crc)
	binary.Write(&buf, binary.BigEndian, msg.QueueId)
	binary.Write(&buf, binary.BigEndian, msg.QueueOffset)
	binary.Write(&buf, binary.BigEndian, cl.currentOffset)
	binary.Write(&buf, binary.BigEndian, msg.BornTimestamp)
	binary.Write(&buf, binary.BigEndian, bodyLen)
	binary.Write(&buf, binary.BigEndian, bodyBytes)
	binary.Write(&buf, binary.BigEndian, topicLen)
	binary.Write(&buf, binary.BigEndian, topicBytes)
	// 获取消息体长度
	bufLen := int32(buf.Len())
	if bufLen != totalSize {
		log.Fatal("写入长度与计算长度不相同")
	}
	// 如何判断消息体是否跨越文件边界
	// 当前的偏移量 + 写入的buf尺寸 大于 文件的最大值
	//if cl.currentFileOffset+totalSize > (cl.lastOffset+CommitLogFileSize) && cl.currentFileOffset > cl.lastOffset {
	if cl.currentFileOffset+int64(totalSize) > CommitLogFileSize {
		var nextFile *os.File
		// 创建下一个文件
		// 构造下一个文件名
		fileIndex := (cl.currentFileOffset + int64(totalSize)) / CommitLogFileSize
		// 打开消息所在文件
		filePath := cl.fileNameAtIndex(fileIndex)
		// 创建下一个文件
		nextFile, err = os.Create(filePath)
		if err != nil {
			return 0, err
		}
		// 计算第一部分消息体长度
		firstPartLen := CommitLogFileSize - cl.currentFileOffset
		// 计算第二部分消息体长度
		secondPartLen := int64(totalSize) - firstPartLen
		// 写入第一部分消息体
		// 因为至少剩余4KB的空间，所以能写入头部的的部分，以及部分消息体内容
		binary.Write(cl.currentFile, binary.BigEndian, buf.Bytes()[:firstPartLen])
		// 写入第二部分消息体
		binary.Write(cl.currentFile, binary.BigEndian, buf.Bytes()[firstPartLen:])
		// 直接刷盘
		err = cl.currentFile.Sync()
		if err != nil {
			return 0, err
		}
		err = nextFile.Sync()
		if err != nil {
			return 0, err
		}
		// 关闭当前文件句柄
		cl.currentFile.Close()
		// 计算出第二部分偏移多少个区块， 更新当前文件的偏移量
		paddingNums := secondPartLen/CommitLogBlockSize + 1
		cl.currentFileOffset = paddingNums * CommitLogBlockSize
		// 更新当前文件
		cl.currentFilePath = filePath
		// 更新当前文件句柄
		cl.currentFile = nextFile

	} else {

		// 写入消息体长度、校验码和消息体
		err = binary.Write(cl.currentFile, binary.BigEndian, buf.Bytes())
		if err != nil {
			return 0, err
		}
		// 如果当前的消息体积不够4k，补全到4k
		padding := CommitLogBlockSize - totalSize
		if padding > 0 {
			pad := make([]byte, padding)
			if err = binary.Write(cl.currentFile, binary.BigEndian, pad); err != nil {
				return 0, err
			}

		}
		// 更新当前文件偏移量，增加4k的偏移量
		cl.currentFileOffset += CommitLogFileSize
		// 直接刷盘
		err = cl.currentFile.Sync()
		if err != nil {
			return 0, err
		}
	}
	// 更新当前最大的偏移量
	var lastOffset = cl.currentOffset
	cl.currentOffset += int64(totalSize)
	// 返回nil表示写入成功
	return lastOffset, nil
}

func (cl *CommitLog) ReadMessage(offset int64) (*protocol.Message, error) {
	// 检查offset是否合法
	if offset < 0 || offset >= cl.currentFileOffset {
		return nil, fmt.Errorf("invalid currentFileOffset %d", offset)
	}
	// 计算消息所在文件和文件内偏移量
	fileIndex := offset / CommitLogFileSize
	fileOffset := offset % CommitLogFileSize
	// 打开消息所在文件
	filePath := cl.fileNameAtIndex(fileIndex)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// 定位到消息所在位置
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	var (
		totalBytes []byte
	)
	// 读取消息
	var (
		buf             *bytes.Buffer
		totalSize       int32
		magicNumber     uint32
		crc             int32
		queueId         int32
		queueOffset     int64
		commitLogOffset int64
		bornTimestamp   int64
		bodyLen         int32
		bodyBytes       []byte
		topicLen        int8
		topicBytes      []byte
	)

	// 读取消息总长度

	err = binary.Read(file, binary.BigEndian, &totalSize)
	if err != nil {
		return nil, err
	}
	totalBytes = make([]byte, totalSize)
	// 判断是否跨文件，如果是跨文件先从两个文件拿到数据，拼接在一起
	if offset+int64(totalSize) > CommitLogFileSize {
		var nextFile *os.File
		var firstPartBytes []byte
		var secondPartLenBytes []byte
		// 计算下一个文件名
		nextFileIndex := (cl.currentFileOffset + int64(totalSize)) / CommitLogFileSize
		// 打开消息所在文件
		nextFilePath := cl.fileNameAtIndex(nextFileIndex)
		nextFile, err = os.Open(nextFilePath)
		if err != nil {
			return nil, err
		}
		// 根据长度计算出，需要取下一个文件的多少个区块
		// 计算第一部分消息体长度
		firstPartLen := CommitLogFileSize - offset
		// 计算第二部分消息体长度
		secondPartLen := int64(totalSize) - firstPartLen
		paddingNums := secondPartLen/CommitLogBlockSize + 1
		secondPartLenBytes = make([]byte, paddingNums*CommitLogBlockSize)
		_, err = io.ReadFull(nextFile, secondPartLenBytes)
		if err != nil {
			return nil, err
		}
		_, err = file.ReadAt(firstPartBytes, offset)
		if err != nil {
			return nil, err
		}
		totalBytes = append(totalBytes, firstPartBytes...)
		totalBytes = append(totalBytes, secondPartLenBytes...)
		if int32(len(totalBytes)) != totalSize {
			return nil, errors.New("读取长度异常")
		}
	} else {
		_, err = file.ReadAt(totalBytes, offset)
		if err != nil {
			return nil, err
		}
	}

	buf = bytes.NewBuffer(totalBytes)

	if err = binary.Read(buf, binary.BigEndian, &totalSize); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &magicNumber); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &crc); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &queueId); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &queueOffset); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &commitLogOffset); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &bornTimestamp); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &bodyLen); err != nil {
		return nil, err
	}
	bodyBytes = make([]byte, bodyLen)
	if err = binary.Read(buf, binary.BigEndian, &bodyBytes); err != nil {
		return nil, err
	}
	if err = binary.Read(buf, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}
	topicBytes = make([]byte, topicLen)
	if err = binary.Read(buf, binary.BigEndian, &topicBytes); err != nil {
		return nil, err
	}

	var msg protocol.Message
	err = proto.Unmarshal(bodyBytes, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (cl *CommitLog) fileNameAtIndex(index int64) string {
	return filepath.Join(filepath.Dir(cl.currentFilePath), fmt.Sprintf("%020d", index*CommitLogFileSize))
}

func main() {
	commitLog, err := NewCommitLog()
	if err != nil {
		log.Fatal(err)
	}
	//message := protocol.Message{
	//	Topic:          "test",
	//	Body:           []byte("123"),
	//	BornTimestamp:  12312312,
	//	StoreTimestamp: 0,
	//	MsgId:          "xxxx2",
	//	ProducerGroup:  "123",
	//	ConsumerGroup:  "123",
	//}
	//// 写文件
	//commitLogOffset, err := commitLog.WriteMessage(&message)
	//if err != nil {
	//	log.Fatal(err)
	//} else {
	//	log.Println("获得offset:", commitLogOffset)
	//}

	// 读文件
	message, err := commitLog.ReadMessage(0)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("获得message: %+v", message)
}
