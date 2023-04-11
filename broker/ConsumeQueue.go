package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

const (
	CONSUME_QUEUE_UNIT_SIZE     = 20      // 文件的单个消息的体积是20，单个文件可以存储30万个消息
	CONSUME_QUEUE_FILE_MAX_SIZE = 6000000 // 单个consumeQueue文件默认大小
	ConsumeQueueBaseDir         = "./store/consume_queue"
)

type ConsumeQueue struct {
	baseDir          string // consumeQueue所在目录
	topic            string
	queueId          int
	messageChan      chan *protocol.Message
	file             *os.File
	writePos         int64 // 当前写指针位置
	pathDir          string
	getWriteFileLock sync.Mutex //
	writeFileLock    sync.Mutex //
	maxQueueOffset   int64
}
type consumeQueueUnit struct {
	commitLogOffset int64
	size            int32
	tagHashCode     int64
}

func NewConsumeQueue(topic string, queueId int) (*ConsumeQueue, error) {
	cq := &ConsumeQueue{
		baseDir:          ConsumeQueueBaseDir,
		topic:            topic,
		queueId:          queueId,
		getWriteFileLock: sync.Mutex{},
		writeFileLock:    sync.Mutex{},
		messageChan:      make(chan *protocol.Message),
	}
	// 检查目录是否存在，不存在则创建
	cq.pathDir = fmt.Sprintf("%s/%s/%d", cq.baseDir, topic, queueId)
	if _, err := os.Stat(cq.pathDir); os.IsNotExist(err) {
		if err := os.MkdirAll(cq.pathDir, 0755); err != nil {
			return nil, err
		} else {
			if _, err := cq.getWriteFile(); err != nil {
				Log.Fatal("NewConsumeQueue error:", err)
			}
		}
	}

	// 获取当前队列的最大偏移量
	if files, err := ioutil.ReadDir(cq.pathDir); err != nil {
		return nil, err
	} else {
		var max int64
		var maxFileInfo fs.FileInfo
		for _, f := range files {
			if !f.IsDir() {
				name := f.Name()
				num, err := strconv.ParseInt(name, 10, 64)
				if err != nil {
					Log.Fatal(err)
				} else {
					if num >= max {
						max = num
						maxFileInfo = f
					}
				}
			}
		}
		if maxFileInfo == nil {
			Log.Fatal("NewConsumeQueue error:", err)
		}
		if maxNum, err := strconv.ParseInt(maxFileInfo.Name(), 10, 64); err != nil {
			Log.Fatal("NewConsumeQueue error:", err)
		} else {
			cq.maxQueueOffset = maxNum + maxFileInfo.Size()/CONSUME_QUEUE_UNIT_SIZE
		}

	}

	return cq, nil
}

// 由commitLog去写入
// todo 当写失败的时候，需要commitLog去重试
// 要返回写入的queueOffset
func (cq *ConsumeQueue) write(commitLogOffset int64, size int32, tagHashCode int64) (int64, error) {
	cq.writeFileLock.Lock()
	defer cq.writeFileLock.Unlock()
	Log.Printf("开始写入ConsumeQueue；commitLogOffset:%d, size:%d, tagHashCode:%d", commitLogOffset, size, tagHashCode)
	if size < 1 {
		Log.Fatal("写入ConsumeQueue数据异常")
	}
	var lastOffset = cq.maxQueueOffset

	// 写入consumeQueue数据
	buf := bytes.NewBuffer([]byte{})
	if err := binary.Write(buf, binary.BigEndian, commitLogOffset); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, size); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, tagHashCode); err != nil {
		return 0, err
	}
	file, err := cq.getWriteFile()

	defer func() {
		// 刷盘
		err = file.Sync()
		if err != nil {
			Log.Fatal(err)
		}
		err = file.Close()
		if err != nil {
			Log.Fatal(err)
		}
	}()
	if err != nil {
		Log.Errorf("write error: ", err)
		return 0, err
	} else {
		if _, err := file.Write(buf.Bytes()); err != nil {
			return 0, err
		} else {
			Log.Println("写入成功ConsumeQueues数据成功:", commitLogOffset, size, tagHashCode)
		}
	}
	cq.maxQueueOffset += 1
	return lastOffset, nil
}

/**
获取即将写入的文件
如果当前文件已经满了，要新建下一个文件
*/
func (cq *ConsumeQueue) getWriteFile() (*os.File, error) {
	cq.getWriteFileLock.Lock()
	defer cq.getWriteFileLock.Unlock()
	files, err := ioutil.ReadDir(cq.pathDir)
	var openPath string
	if err != nil {
		panic(err)
	}
	var max int64
	var fileCurrentSize int64
	var currentFile fs.FileInfo
	if len(files) == 0 {
		// 如果不存在文件，就
		openPath = fmt.Sprintf("%s/%s", cq.pathDir, "00000000000000000000")
	} else {
		for _, f := range files {
			if !f.IsDir() {
				name := f.Name()
				num, err := strconv.ParseInt(name, 10, 64)
				if err != nil {
					Log.Fatal(err)
				} else {
					if num >= max {
						max = num
						currentFile = f
						fileCurrentSize = f.Size()
					}
				}
			}
		}
		// 如果当前最新的文件，已经写不下了，创建下一个
		if fileCurrentSize >= CONSUME_QUEUE_FILE_MAX_SIZE {
			fileName := fmt.Sprintf("%020d", max+CONSUME_QUEUE_FILE_MAX_SIZE)
			openPath = fmt.Sprintf("%s/%s", cq.pathDir, fileName)
		} else {
			openPath = fmt.Sprintf("%s/%s", cq.pathDir, currentFile.Name())
		}
	}
	file, err := os.OpenFile(openPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		Log.Errorf("getWriteFile error: %+v", err)
		return nil, err
	}
	if stat, err := file.Stat(); err != nil {
		Log.Fatal(err)
	} else {
		Log.Println("getWriteFile:", openPath, stat.Size())
		if stat.Size() >= CONSUME_QUEUE_FILE_MAX_SIZE {
			Log.Fatalf("文件超出了限制 %d", fileCurrentSize)
		}

	}

	return file, nil
}

// 打开下一个consumeQueue文件，文件名为初始偏移量
func (cq *ConsumeQueue) getReadFile(consumeOffset int64) (*os.File, error) {
	// 获取当前consumeOffset应该所在的文件
	fileIndex := consumeOffset * CONSUME_QUEUE_UNIT_SIZE / CONSUME_QUEUE_FILE_MAX_SIZE // 计算商
	fileName := fmt.Sprintf("%020d", fileIndex*CONSUME_QUEUE_FILE_MAX_SIZE)            // 根据文件名称规则，计算出文件名
	filePath := fmt.Sprintf("%s/%s/%d/%s", cq.baseDir, cq.topic, cq.queueId, fileName)
	Log.Debugf("getReadFile: %s", filePath)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		Log.Errorf("getReadFile, error %+v", err)
		return nil, err
	}
	return file, nil
}

// 读取consumeQueue数据
func (cq *ConsumeQueue) read(consumeOffset int64) (*consumeQueueUnit, error) {
	if consumeOffset > cq.maxQueueOffset {
		return nil, ErrMessageNotYet
	}
	unit := &consumeQueueUnit{}
	file, err := cq.getReadFile(consumeOffset)
	if err != nil {
		Log.Errorf("read error: %+v", err)
		return nil, err
	}
	defer file.Close()
	// 计算要读取的偏移量 600 * 20 % 600000
	readOffset := (consumeOffset * CONSUME_QUEUE_UNIT_SIZE) % CONSUME_QUEUE_FILE_MAX_SIZE
	Log.Debugf("read offset: %d", readOffset)
	if _, err := file.Seek(readOffset, 0); err != nil {
		return nil, err
	}
	// 读取consumeQueue数据
	data := make([]byte, CONSUME_QUEUE_UNIT_SIZE)
	if _, err := file.Read(data); err != nil {
		if err == io.EOF {
			// 这里已经读到结尾了，就是还没有消息
			return nil, ErrMessageNotYet
		} else {
			// 读取出现错误，处理错误信息
			return nil, err
		}
	}
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.BigEndian, &unit.commitLogOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &unit.size); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.BigEndian, &unit.tagHashCode); err != nil {
		return nil, err
	}

	Log.Debugf("读取consumeQueue的结果为%+v", unit)
	if unit.size < 1 {
		Log.Fatalf("读取consumeQueue的数据异常")
	}
	return unit, nil
}

// 读取consumeQueue数据
func (cq *ConsumeQueue) readBatch(consumeOffset int64, nums int) ([]*consumeQueueUnit, error) {
	Log.Printf("readBatch begin")

	if consumeOffset > cq.maxQueueOffset {
		return nil, ErrMessageNotYet
	}

	var end int64
	if consumeOffset+int64(nums) > cq.maxQueueOffset {
		end = cq.maxQueueOffset
	} else {
		end = consumeOffset + int64(nums)
	}
	file, err := cq.getReadFile(consumeOffset)
	if err != nil {
		Log.Errorf("read error: %+v", err)
		return nil, err
	}
	defer file.Close()
	var ret = make([]*consumeQueueUnit, 0)
	for {
		var unit consumeQueueUnit
		if consumeOffset >= end {
			break
		}
		// 计算要读取的偏移量 600 * 20 % 600000
		readOffset := (consumeOffset * CONSUME_QUEUE_UNIT_SIZE) % CONSUME_QUEUE_FILE_MAX_SIZE
		Log.Debugf("read offset: %d", readOffset)
		if _, err := file.Seek(readOffset, 0); err != nil {
			return nil, err
		}
		// 读取consumeQueue数据
		data := make([]byte, CONSUME_QUEUE_UNIT_SIZE)
		if _, err := file.Read(data); err != nil {
			if err == io.EOF {
				// 这里已经读到结尾了，就是还没有消息
				return nil, ErrMessageNotYet
			} else {
				// 读取出现错误，处理错误信息
				return nil, err
			}
		}
		buf := bytes.NewBuffer(data)
		if err := binary.Read(buf, binary.BigEndian, &unit.commitLogOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.BigEndian, &unit.size); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.BigEndian, &unit.tagHashCode); err != nil {
			return nil, err
		}

		Log.Debugf("读取consumeQueue的结果为%+v", unit)
		if unit.size < 1 {
			Log.Fatalf("读取consumeQueue的数据异常")
		}
		ret = append(ret, &unit)
		consumeOffset += 1
	}

	return ret, nil
}

// 计算出总共有多少消息
func (cq *ConsumeQueue) Count() (int64, error) {
	// 读取所有的文件
	var totalFileSize int64
	if files, err := ioutil.ReadDir(cq.pathDir); err != nil {
		return 0, err
	} else {
		for _, f := range files {
			if !f.IsDir() {
				// 合计大小
				totalFileSize += f.Size()
			}
		}
	}

	// 除以单个文件的大小
	return totalFileSize / CONSUME_QUEUE_UNIT_SIZE, nil
}
