package broker

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

var (
	TopicDbFilePath string = "./store/topic.db"
)

type TopicRouteRecord struct {
	topic         string
	consumerGroup string
	queueId       int
	clientId      string
	offset        int64
}

type consumerGroupInfo struct {
	ConsumerGroupName string        `json:"consumer_group_name"`
	QueueInfo         map[int]int64 `json:"queue_info"` // queueId -> offset
}

type topicInfo struct {
	TopicName      string                        `json:"topic_name"`
	ConsumerGroups map[string]*consumerGroupInfo `json:"consumer_group"`
}

type TopicRouteManager struct {
	table      []*TopicRouteRecord
	topicsFile *os.File
	topicDb    map[string]*topicInfo
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
			log.Printf("创建消费组成功：+%v", topic.ConsumerGroups[consumerGroupName])
		}
	}
	return nil
}

// 注册路由信息
// 新的消费者注册的时候，队列填写为空， 由rebalance重新分配
func (t *TopicRouteManager) RegisterConsumer(topicName string, consumerGroupName string, clientId string, conn net.Conn) error {
	client := Client{
		Conn:          conn,
		ClientID:      clientId,
		LastHeartbeat: time.Now(),
		Status:        1,
	}
	log.Println("开始注册消费者；+%v", client)
	t.topics[topicName].consumerGroups[consumerGroupName].Clients[clientId] = &client
	return t.ReBalanceByRegister(topicName, consumerGroupName)
}

// 重新分配队列
func (t *TopicRouteManager) ReBalanceByRegister(topicName string, consumerGroupName string) error {
	// 获取所有的队列
	var topicRouteRecords []*TopicRouteRecord
	for _, record := range t.table {
		if record.topic == topicName && record.consumerGroup == consumerGroupName {
			topicRouteRecords = append(topicRouteRecords, record)
		}
	}

	// 获取正常可用的客户端
	var clients []*Client

	for _, c := range t.topics[topicName].consumerGroups[consumerGroupName].Clients {
		if c.Status == 1 {
			clients = append(clients, c)
		}
	}
	topicRouteRecordsSize := len(topicRouteRecords)
	clientsSize := len(clients)
	log.Println("ReBalanceByRegister - topicRouteRecordsSize：%d, clientsSize:%d", topicRouteRecordsSize, clientsSize)
	//重新分配
	//分配策略: 获取现在所有的消费组中所有的clientId，做平均分配
	var targetClients []*Client
	// 如果队列的数量比客户端多，先每个客户端分配一个，然后随机冲clients抽计入进去
	// todo 简单的分配策略
	if topicRouteRecordsSize >= clientsSize {
		targetClients = append(targetClients, clients...)
		for i := 0; i < topicRouteRecordsSize-clientsSize; i++ {
			targetIndex := rand.Intn(len(clients))
			targetClients = append(targetClients, clients[targetIndex])
		}
	} else {
		for i := 0; i < clientsSize; i++ {
			targetClients = append(targetClients, clients[i])
		}
	}
	for i, r := range topicRouteRecords {
		r.clientId = targetClients[i].ClientID
	}
	return nil
}

// 获取当前clientId 的分配队列
func (t *TopicRouteManager) GetTopicRouteInfo() {

}

func (t *TopicRouteManager) Init() (map[string]*Topic, error) {
	t.topicDb = make(map[string]*topicInfo)
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
				t.topics[topicName] = NewTopic(topicName)
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
							clientId:      "",
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
	info := topicInfo{
		TopicName:      topicName,
		ConsumerGroups: make(map[string]*consumerGroupInfo, 0),
	}
	var topicMap = make(map[string]topicInfo)
	topicMap[topicName] = info
	if _, ok := t.topicDb[topicName]; ok {
		log.Println("新增失败，主题已经存在")
		return nil
	} else {
		t.topicDb[topicName] = &topicInfo{
			TopicName:      topicName,
			ConsumerGroups: make(map[string]*consumerGroupInfo, 0),
		}
		t.topics[topicName] = NewTopic(topicName)
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

func (t *TopicRouteManager) RemoveClient(clientId string) {
	log.Println("开始删除Client:%s", clientId)
	// 删除掉topic中的client
	log.Println("删除掉topic中的client:%s", clientId)
	for _, topic := range t.topics {
		for _, c := range topic.consumerGroups {
			if _, ok := c.Clients[clientId]; ok {
				// 删除
				delete(c.Clients, clientId)
			}
		}
	}
	log.Println("删除topicTable中的client:%s", clientId)
	// 删除topicTable中的client
	for _, r := range t.table {
		if r.clientId == clientId {
			r.clientId = ""
		}
	}
}

func (t *TopicRouteManager) GetTopicTableRecord(topicName string, consumerGroupName string, clientId string, qid int32) (*TopicRouteRecord, error) {
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
