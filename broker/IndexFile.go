package broker

import "os"

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
	Key     int64       // slot 对应的 key，由 top 和 messageId 计算而来
	Payload IndexRecord // slot 最后一条消息的索引记录数据
}

// IndexFile 存储索引文件相关信息及操作
type IndexFile struct {
	file           *os.File        // 文件指针
	header         IndexHeader     // Header 信息
	slotBaseOffset int64           // Index 文件中第一个哈希槽的位置
	slotMap        map[int64]*Slot // map 用于缓存哈希槽数据
	lastSlotNum    int32           // 索引文件中最新的哈希槽数量
}

func (i IndexHeader) name() {

}
