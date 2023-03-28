package broker

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
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
	//CommitLogFileSize = 50 * 1024 * 4
	// CommitLog区块大小
	CommitLogBlockSize = 4 * 1024 //4096
	// 文件名长度
	fileNameLength = 20
	// MagicNumber 消息开始的标识
	MagicNumber uint32 = 0xdabbcf1c
	// 每个 CommitLog 文件的最大大小
	commitLogDir = "./store/commit_log"
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
	var fileNums int
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
		fileNums = 1
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
			fileNums = len(files)
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
	commitLog := CommitLog{
		currentFilePath:   filePath,
		currentFile:       file,
		currentFileOffset: stat.Size(),
		writeLock:         sync.Mutex{},
		currentOffset:     int64(fileNums-1)*CommitLogFileSize + stat.Size(),
	}
	log.Printf("初始化 CommitLog：%+v", commitLog)
	return &commitLog, nil
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
		lastOffset int64
	)

	// 先全部写入一个buf中，计算出总长度
	// 计算消息长度
	bodyBytes, err = proto.Marshal(msg)
	bodyLen = int32(len(bodyBytes))

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
	} else {
		//log.Printf("准备写入文件长度为:%d", bufLen)
	}
	// 如何判断消息体是否跨越文件边界
	// 当前的偏移量 + 写入的buf尺寸 大于 文件的最大值
	//if cl.currentFileOffset+totalSize > (cl.lastOffset+CommitLogFileSize) && cl.currentFileOffset > cl.lastOffset {
	if cl.currentFileOffset+int64(totalSize) > CommitLogFileSize {
		//log.Println("跨文件写入")
		var nextFile *os.File
		// 创建下一个文件
		// 构造下一个文件名
		fileIndex := (cl.currentOffset + int64(totalSize)) / CommitLogFileSize
		// 打开消息所在文件
		nextFilePath := cl.fileNameAtIndex(fileIndex)
		// 创建下一个文件
		nextFile, err = os.Create(nextFilePath)
		if err != nil {
			return 0, err
		}
		// 计算第一部分消息体长度
		firstPartLen := CommitLogFileSize - cl.currentFileOffset
		firstPartBytes := buf.Bytes()[:firstPartLen]
		// 计算第二部分消息体长度
		secondPartLen := int64(totalSize) - firstPartLen
		secondPartBytes := buf.Bytes()[firstPartLen:]
		// 写入第一部分消息体
		// 因为至少剩余4KB的空间，所以能写入头部的的部分，以及部分消息体内容
		log.Printf("写入第一部分的长度为:%d, md5:%d", len(firstPartBytes), md5.Sum(firstPartBytes))
		binary.Write(cl.currentFile, binary.BigEndian, firstPartBytes)
		// 写入第二部分消息体
		log.Printf("写入第二部分的长度为:%d, md5:%d", len(secondPartBytes), md5.Sum(secondPartBytes))
		binary.Write(nextFile, binary.BigEndian, secondPartBytes)
		// 第二部分消息体补全区块
		padding := CommitLogBlockSize - (secondPartLen % CommitLogBlockSize)
		if padding > 0 {
			log.Printf("第二部分的尾部补全区块长度:%d", padding)
			pad := make([]byte, padding)
			if err = binary.Write(nextFile, binary.BigEndian, pad); err != nil {
				return 0, err
			}

		}
		log.Printf("body 内容的md5:%d", md5.Sum(bodyBytes))
		// 直接刷盘
		//log.Println("当前文件刷盘:", cl.currentFilePath)
		err = cl.currentFile.Sync()
		if err != nil {
			return 0, err
		}
		//log.Println("下一个文件刷盘:", nextFilePath)
		err = nextFile.Sync()
		if err != nil {
			return 0, err
		}
		// 关闭当前文件句柄
		cl.currentFile.Close()
		// 计算出第二部分偏移多少个区块， 更新当前文件的偏移量
		paddingNums := secondPartLen/CommitLogBlockSize + 1
		cl.currentFileOffset = paddingNums * CommitLogBlockSize
		lastOffset = cl.currentOffset
		cl.currentOffset += int64(len(firstPartBytes)) + int64(len(secondPartBytes)) + padding
		// 更新当前文件
		cl.currentFilePath = nextFilePath
		// 更新当前文件句柄
		cl.currentFile = nextFile

	} else {
		//log.Println("当前文件写入")
		// 写入消息体长度、校验码和消息体
		err = binary.Write(cl.currentFile, binary.BigEndian, buf.Bytes())
		if err != nil {
			return 0, err
		}
		if totalSize > CommitLogBlockSize {
			// 文件超过了4k大小
			padding := CommitLogBlockSize - (totalSize % CommitLogBlockSize)
			log.Printf("文件超过了阈值, padding: %d", padding)
			if padding > 0 {
				pad := make([]byte, padding)
				if err = binary.Write(cl.currentFile, binary.BigEndian, pad); err != nil {
					return 0, err
				}
			}
			// 更新当前文件偏移量，增加4k的偏移量
			cl.currentFileOffset += int64(totalSize) + int64(padding)
			lastOffset = cl.currentOffset
			cl.currentOffset += int64(totalSize) + int64(padding)
			// 直接刷盘
			err = cl.currentFile.Sync()
			log.Printf("写入总长度为%d", int64(totalSize)+int64(padding))
			log.Printf("body 内容的md5:%d", md5.Sum(bodyBytes))
			if err != nil {
				return 0, err
			}
		} else {
			// 如果当前的消息体积不够4k，补全到4k
			padding := CommitLogBlockSize - totalSize
			if padding > 0 {
				pad := make([]byte, padding)
				if err = binary.Write(cl.currentFile, binary.BigEndian, pad); err != nil {
					return 0, err
				}

			}
			// 更新当前文件偏移量，增加4k的偏移量
			cl.currentFileOffset += CommitLogBlockSize
			lastOffset = cl.currentOffset
			cl.currentOffset += CommitLogBlockSize
			// 直接刷盘
			err = cl.currentFile.Sync()
			if err != nil {
				return 0, err
			}
		}

	}
	log.Printf("结果：currentFileOffset：%d, currentOffset: %d, lastOffset: %d", cl.currentFileOffset, cl.currentOffset, lastOffset)

	if lastOffset%CommitLogBlockSize != 0 {
		log.Fatal("写入异常，偏移量错误:", lastOffset)
	}

	// 返回nil表示写入成功
	return lastOffset, nil
}

func (cl *CommitLog) ReadMessage(offset int64) (*protocol.Message, error) {
	//log.Printf("开始读取%d的消息", offset)
	// 检查offset是否合法
	if offset < 0 || offset >= cl.currentOffset {
		return nil, fmt.Errorf("invalid currentFileOffset %d", offset)
	}
	// 计算消息所在文件和文件内偏移量
	fileIndex := offset / CommitLogFileSize
	fileOffset := offset % CommitLogFileSize
	log.Printf("计算得出当前文件的偏移量：%d", fileOffset)
	// 打开消息所在文件
	filePath := cl.fileNameAtIndex(fileIndex)
	targetFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer targetFile.Close()
	// 定位到消息所在位置
	log.Printf("计算得到fileOffset：%d, 文件位置:%s", fileOffset, filePath)
	_, err = targetFile.Seek(fileOffset, io.SeekStart)
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

	err = binary.Read(targetFile, binary.BigEndian, &totalSize)
	if err != nil {
		log.Printf("读取totalSize异常")
		return nil, err
	} else {
		//log.Printf("准备读取totalSize:%d", totalSize)
	}
	totalBytes = make([]byte, totalSize)
	// 判断是否跨文件，如果是跨文件先从两个文件拿到数据，拼接在一起
	if fileOffset+int64(totalSize) >= CommitLogFileSize {
		log.Println("跨文件读取")
		var nextFile *os.File
		var firstPartBytes []byte
		var secondPartBytes []byte
		// 计算下一个文件名
		nextFileIndex := (cl.currentOffset + int64(totalSize)) / CommitLogFileSize
		// 打开消息所在文件
		nextFilePath := cl.fileNameAtIndex(nextFileIndex)
		nextFile, err = os.Open(nextFilePath)
		if err != nil {
			return nil, err
		} else {
			log.Printf("成功打开下一个文件:%s", nextFilePath)
		}
		defer nextFile.Close()
		// 根据长度计算出，需要取下一个文件的多少个区块
		// 计算第一部分消息体长度
		// -------------------- | --------------------
		//   CommitLogFileSize
		firstPartLen := CommitLogFileSize - fileOffset
		firstPartBytes = make([]byte, firstPartLen)
		// 读取第一部分的内容
		_, err = targetFile.Seek(fileOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		err = binary.Read(targetFile, binary.BigEndian, &firstPartBytes)

		if err != nil {
			log.Println("read firstPartBytes error:", err)
			return nil, err
		}
		log.Printf("读取完第一部分的长度为%d，md5:%d", len(firstPartBytes), md5.Sum(firstPartBytes))
		// 计算第二部分消息体长度
		secondPartLen := int64(totalSize) - firstPartLen
		//paddingNums := secondPartLen/CommitLogBlockSize + 1
		// 生成第二部分的区块
		//secondPartBytes = make([]byte, paddingNums*CommitLogBlockSize)
		secondPartBytes = make([]byte, secondPartLen)
		_, err = nextFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
		err = binary.Read(nextFile, binary.BigEndian, secondPartBytes)
		//_, err = nextFile.ReadAt(secondPartBytes, 0)
		//_, err = io.ReadFull(nextFile, secondPartBytes)
		if err != nil {
			log.Println("read secondPartBytes error:", err)
			return nil, err
		}
		log.Printf("读取完第二部分的长度为%d, md5:%d", len(secondPartBytes), md5.Sum(secondPartBytes))
		// 两部分拼接再一起
		totalBytes = make([]byte, 0)
		totalBytes = append(totalBytes, firstPartBytes...)
		totalBytes = append(totalBytes, secondPartBytes...)
		log.Printf("最后获得文件长度:%d", len(totalBytes))
		//totalBytes = totalBytes[:totalSize]
	} else {
		log.Printf("当前文件读取: %s", cl.currentFilePath)
		// 再偏移到当前文件
		_, err = targetFile.Seek(fileOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		_, err = targetFile.Read(totalBytes)
		if err != nil {
			if err == io.EOF {
				log.Println("read totalBytes error:", err)
				return nil, err
			}
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
	} else {
		//log.Printf("body 内容的md5:%d", md5.Sum(bodyBytes))
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
		log.Printf("body长度为%d", bodyLen)
		log.Printf("topic长度为%d", topicLen)
		log.Printf("反序列化body异常 error: %v", err)
		return nil, err
	}
	return &msg, nil
}

func (cl *CommitLog) fileNameAtIndex(index int64) string {
	return filepath.Join(filepath.Dir(cl.currentFilePath), fmt.Sprintf("%020d", index*CommitLogFileSize))
}
