package broker

import (
	"encoding/binary"
	"io"
	"log"
	"math"
	"os"
)

// 常量定义
const (
	IndexFileHeaderLength = 40       // Index 文件头的长度（单位：字节）
	SlotNum               = 5000000  // 哈希槽数量
	IndexNum              = 20000000 // 最大索引记录数量
	HashSlotSize          = 4        // 单个哈希槽在索引文件中占用的字节数
	IndexRecordLength     = 20       // 单条索引记录占用的字节数
)

// IndexHeader 存储 RocketMQ index 文件的头部信息
type IndexHeader struct {
	BeginTimestamp int64 // 存储前索引文件内，所有消息的最小时间
	EndTimestamp   int64 // 存储前索引文件内，所有消息的最大时间
	MinPhyOffset   int64 // 存储前索引文件内，所有消息的最小物理偏移量
	MaxPhyOffset   int64 // 存储前索引文件内，所有消息的最大物理偏移量
	ValidSlotNum   int32 // 有效 hash slot 数量
	IndexNum       int32 // 索引数量
}

// IndexRecord 存储 RocketMQ index 文件中单个索引记录的字段
type IndexRecord struct {
	HashValue int32  // 哈希值
	PhyOffset int64  // 物理偏移量
	Timediff  int32  // 储存时间差
	SlotPrev  uint32 // 当前哈希槽内上一条索引的位置
}

func InitIndexFile() {

}

// Slot 单个哈希槽，用于存储当前哈希槽最后一条消息的索引记录
type Slot struct {
	LastIndexOffset int64 // slot 对应的 key，由 top 和 messageId 计算而来
}

// IndexFile 存储索引文件相关信息及操作
type IndexFile struct {
	file           *os.File        // 文件指针
	header         IndexHeader     // Header 信息
	slotBaseOffset int64           // Index 文件中第一个哈希槽的位置
	slotMap        map[int64]*Slot // map 用于缓存哈希槽数据
	lastSlotNum    int32           // 索引文件中最新的哈希槽数量
}

/**
获取最后一个索引文件。如果没有索引文件或者最后一个索引文件满了，那么创建一个新的文件
检查索引文件列表最后一个文件是否存在以及是否满
如果不存在或者已经满了，创建新的文件，并且把上一个索引文件异步刷盘
如果最后一个文件存在并且未满，直接返回该文件
*/
func (f *IndexFile) getAndCreateLastIndexFile(indexRecord *IndexRecord) error {

}

func (f *IndexFile) writeIndex(indexRecord *IndexRecord) error {
	var err error
	key := int64(indexRecord.HashValue % SlotNum)

	// 读取当前位置，看是否非0，如果非0，代表之前有数据写入，需要解决哈希冲突
	// 当前哈希槽的 payload
	slotOffset := int64(key) * HashSlotSize
	var slotLastIndexOffset int64
	_, err = f.file.Seek(slotOffset, io.SeekStart)
	if err != nil {
		log.Println("IndexFile writeIndex seek error:", err)
		return err
	}
	err = binary.Read(f.file, binary.BigEndian, &slotLastIndexOffset)
	if err != nil {
		log.Println("read slotLastIndexOffset error:", err)
		return err
	}
	if slotLastIndexOffset == 0 {
		// 还没有写入过

	} else {
		// 已经写入了
	}
	// 如果当前哈希槽还没有数据，则创建一个新的槽
	if f.slotMap[key] == nil {
		f.slotMap[key] = &Slot{Key: key, Payload: indexRecord}
		f.lastSlotNum = int(math.Max(float64(f.lastSlotNum), float64(key)))
	} else {
		// 否则，将数据追加到哈希槽的末尾
		last := f.slotMap[key].Payload
		if last != nil {
			last.SlotPrev = uint32(f.lastIndexPos)
			if _, err := f.file.WriteAt(f.indexRecord2Bytes(indexRecord), f.lastIndexPos); err != nil {
				return err
			}
			f.lastIndexPos += IndexRecordLength
		}
		f.slotMap[key].Payload = indexRecord
	}

	return nil
}

// indexRecord2Bytes 将 IndexRecord 结构体转换成字节切片，方便进行文件写入操作
func (i *IndexFile) indexRecord2Bytes(indexRecord *IndexRecord) []byte {
	buf := make([]byte, IndexRecordLength)
	binary.BigEndian.PutUint32(buf[0:4], uint32(indexRecord.HashValue))
	binary.BigEndian.PutUint64(buf[4:12], uint64(indexRecord.PhyOffset))
	binary.BigEndian.PutUint32(buf[12:16], uint32(indexRecord.Timediff))
	binary.BigEndian.PutUint32(buf[16:20], uint32(indexRecord.SlotPrev))
	return buf
}
func (i *IndexFile) readIndex() {

}

func (i *IndexFile) flush() {

}
