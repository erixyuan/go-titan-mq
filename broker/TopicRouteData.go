package broker

import (
	"encoding/json"
	"errors"
	"github.com/erixyuan/go-titan-mq/protocol"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

var (
	TopicDbFilePath string = "./store/topic.db"
)

type TopicRouteRecord struct {
	topic           string
	consumerGroup   string
	queueId         int
	clientId        string
	offset          int64
	lastConsumeTime time.Time
}

type consumerGroupInfo struct {
	ConsumerGroupName string        `json:"consumer_group_name"`
	QueueInfo         map[int]int64 `json:"queue_info"` // queueId -> offset
}

type topicDbRecord struct {
	TopicName      string                        `json:"topic_name"`     // 队列的名称
	ConsumerGroups map[string]*consumerGroupInfo `json:"consumer_group"` // 消费组
	Queue          []int                         `json:"queue"`          //队列
}

type TopicRouteManager struct {
	table      []*TopicRouteRecord
	topicsFile *os.File
	topicDb    map[string]*topicDbRecord
	topics     map[string]*Topic
	flushLock  sync.Mutex
}

func (t *TopicRouteManager) RegisterConsumerGroup(topicName string, consumerGroupName string) error {
	if topic, ok := t.topicDb[topicName]; !ok {
		return errors.New("不存在的主题")
	} else {
		if _, ok = topic.ConsumerGroups[consumerGroupName]; ok {
			return errors.New("消费组名称已经存在")
		} else {
			var queueInfo = make(map[int]int64)
			for i := 0; i < QueueNumsDefault; i++ {
				queueInfo[i] = 0
			}
			topic.ConsumerGroups[consumerGroupName] = &consumerGroupInfo{
				ConsumerGroupName: consumerGroupName,
				QueueInfo:         queueInfo,
			}
			if err := t.Flush(); err != nil {
				return err
			}

			// 更新
			log.Printf("topics %+v", t.topics)
			t.topics[topicName].consumerGroups[consumerGroupName] = &ConsumerGroup{
				TopicName: topicName,
				GroupName: consumerGroupName,
				Clients:   make(map[string]*Client),
			}
			// 更新table
			for qid, _ := range queueInfo {
				t.table = append(t.table, &TopicRouteRecord{
					topic:         topicName,
					consumerGroup: consumerGroupName,
					queueId:       qid,
					clientId:      "",
					offset:        0,
				})
			}
			log.Printf("创建消费组成功：+%v", topic.ConsumerGroups[consumerGroupName])
		}
	}
	return nil
}

// 注册路由信息
// 新的消费者注册的时候，队列填写为空， 由rebalance重新分配
func (t *TopicRouteManager) RegisterConsumer(topicName string, consumerGroupName string, clientId string, conn net.Conn) (*Client, error) {
	client := Client{
		Conn:          conn,
		ClientID:      clientId,
		LastHeartbeat: time.Now(),
		Status:        1,
	}
	log.Printf("RegisterConsumer 开始注册消费者；+%v", client)
	t.topics[topicName].consumerGroups[consumerGroupName].Clients[clientId] = &client
	return &client, nil
}

// 获取当前clientId 的分配队列
func (t *TopicRouteManager) GetTopicRouteInfo() {

}

func (t *TopicRouteManager) Init() (map[string]*Topic, error) {
	t.topicDb = make(map[string]*topicDbRecord)
	t.topics = make(map[string]*Topic) // 返回给给broker
	if topicFile, err := os.OpenFile(TopicDbFilePath, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		log.Fatal(err)
	} else {
		t.topicsFile = topicFile
		if stat, err := t.topicsFile.Stat(); err != nil {
			log.Fatal(err)
		} else {
			if stat.Size() == 0 {
				log.Println("topic.db为空，准备写入空json")
				t.topicsFile.Write([]byte("{}"))
				if err := t.topicsFile.Sync(); err != nil {
					log.Fatal(err)
				} else {
					return t.topics, nil
				}
			}
		}
	}
	if bytes, err := io.ReadAll(t.topicsFile); err != nil {
		return nil, err
	} else {
		if err := json.Unmarshal(bytes, &t.topicDb); err != nil {
			log.Fatal("json.Unmarshal异常：", err)
		} else {
			log.Printf("读取topic.db成功 %+v", t.topicDb)
			flushFlag := false
			for topicName, d := range t.topicDb {
				t.topics[topicName] = RecoverTopic(topicName, len(d.Queue))
				// 初始化消费组
				for consumerGroupName, cg := range d.ConsumerGroups {
					t.topics[topicName].consumerGroups[consumerGroupName] = &ConsumerGroup{
						TopicName: topicName,
						GroupName: consumerGroupName,
						Clients:   make(map[string]*Client),
					}
					for qid, offset := range cg.QueueInfo {
						t.table = append(t.table, &TopicRouteRecord{
							topic:         topicName,
							consumerGroup: consumerGroupName,
							queueId:       qid,
							clientId:      ReBalance,
							offset:        offset,
						})
					}
					// 如果db中的队列数据量，少于现在的，更新db，但是如果在动态过程中调整的呢，增加了队列?
					// 通过事件的方式处理
					if len(t.topics[topicName].ConsumeQueues) > len(cg.QueueInfo) {
						for i := len(cg.QueueInfo); i < len(t.topics[topicName].ConsumeQueues); i++ {
							t.table = append(t.table, &TopicRouteRecord{
								topic:         topicName,
								consumerGroup: consumerGroupName,
								queueId:       i,
								clientId:      "",
								offset:        0,
							})
							cg.QueueInfo[i] = 0
						}
						flushFlag = true // 刷盘
					}
				}
			}
			if flushFlag {
				t.Flush()
			}
		}
	}
	go t.AutoRefresh()
	return t.topics, nil
}

func (t *TopicRouteManager) RegisterTopic(topicName string) error {
	info := topicDbRecord{
		TopicName:      topicName,
		ConsumerGroups: make(map[string]*consumerGroupInfo, 0),
	}
	var topicMap = make(map[string]topicDbRecord)
	topicMap[topicName] = info
	if _, ok := t.topicDb[topicName]; ok {
		log.Println("新增失败，主题已经存在")
		return nil
	} else {
		newTopic := NewTopic(topicName)
		t.topics[topicName] = newTopic
		// 更新db
		dbRecord := topicDbRecord{
			TopicName:      topicName,
			ConsumerGroups: make(map[string]*consumerGroupInfo, 0),
		}
		for i := 0; i < len(newTopic.ConsumeQueues); i++ {
			dbRecord.Queue = append(dbRecord.Queue, i)
		}
		t.topicDb[topicName] = &dbRecord

	}

	if err := t.Flush(); err != nil {
		return err
	}
	log.Println("新增主题成功")
	return nil
}

func (t *TopicRouteManager) Flush() error {
	t.flushLock.Lock()
	t.flushLock.Unlock()
	var err error
	bytes, err := json.Marshal(t.topicDb)
	if err != nil {
		log.Fatal("TopicRouteManager Flush异常：", err)
	}
	err = ioutil.WriteFile(TopicDbFilePath, bytes, 0644)
	return nil
}

// 从路由表中获取当前client分配到的队列
func (t *TopicRouteManager) FindQueueId(topicName string, consumerGroupName string, clientId string) []int32 {
	log.Printf("开始查看client:[%s][%s][%s]分配的queue", topicName, consumerGroupName, clientId)
	var queueIds []int32
	for _, record := range t.table {
		if record.topic == topicName && record.consumerGroup == consumerGroupName && record.clientId == clientId {
			queueIds = append(queueIds, int32(record.queueId))
		}
	}
	return queueIds
}

func (t *TopicRouteManager) FindQueues(topicName string, consumerGroupName string, clientId string) []*protocol.ConsumeProgress {
	log.Printf("开始查看client:[%s][%s][%s]分配的queue", topicName, consumerGroupName, clientId)
	var queues []*protocol.ConsumeProgress
	for _, record := range t.table {
		if record.topic == topicName && record.consumerGroup == consumerGroupName && record.clientId == clientId {
			queues = append(queues, &protocol.ConsumeProgress{
				QueueId: int32(record.queueId),
				Offset:  record.offset,
			})
		}
	}
	return queues
}

func (t *TopicRouteManager) GetTopicTableRecord(topicName string, consumerGroupName string, qid int32, clientId string) (*TopicRouteRecord, error) {
	for _, record := range t.table {
		if record.topic == topicName && record.consumerGroup == consumerGroupName && record.clientId == clientId && int32(record.queueId) == qid {
			return record, nil
		}
	}
	return nil, ErrTopicTableRecordNotFound
}

func (t *TopicRouteManager) AutoRefresh() {
	for {
		t.Flush()
		time.Sleep(5 * time.Second)
	}
}

func (t *TopicRouteManager) GetAllConsumerGroupRecord(topicName string, consumerGroupName string) []*TopicRouteRecord {
	ret := make([]*TopicRouteRecord, 0)
	for _, record := range t.table {
		if record.topic == topicName && record.consumerGroup == consumerGroupName {
			ret = append(ret, record)
		}
	}
	return ret
}
