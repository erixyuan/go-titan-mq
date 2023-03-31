package broker

import (
	"encoding/json"
	"fmt"
	"github.com/erixyuan/go-titan-mq/protocol"
	"github.com/erixyuan/go-titan-mq/tools"
	"github.com/julienschmidt/httprouter"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

type HttpHandler struct {
	broker *Broker
}

func (h *HttpHandler) Index(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {

}

func (h *HttpHandler) AddTopic(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	decoder := json.NewDecoder(request.Body)
	defer request.Body.Close()
	var addTopicReq protocol.AddTopicRequest
	err := decoder.Decode(&addTopicReq)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// user已经成功解析
	// 这里可以将user存储到数据库中，或者做其他操作
	log.Printf("新增主题: %+v", addTopicReq)
	err = h.broker.topicRouteManager.RegisterTopic(addTopicReq.GetTopicName())
	if err != nil {
		log.Printf("AddTopic error: %v", err)
		return
	}
	// 返回成功创建的用户信息
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusCreated)
	json.NewEncoder(writer).Encode(addTopicReq)
}

func (h *HttpHandler) AddConsumerGroup(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	decoder := json.NewDecoder(request.Body)
	defer request.Body.Close()
	var req protocol.AddConsumerGroupRequest
	err := decoder.Decode(&req)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// user已经成功解析
	// 这里可以将user存储到数据库中，或者做其他操作
	log.Printf("新增消费组: %+v", req)
	if req.GetGroupName() == "" || req.GetTopicName() == "" {
		h.Return(writer, "参数异常")
		return
	}
	err = h.broker.topicRouteManager.RegisterConsumerGroup(req.GetTopicName(), req.GetGroupName())
	if err != nil {
		log.Printf("AddConsumerGroup error: %v", err)
		h.Return(writer, err.Error())
	} else {
		go h.broker.doReBalance(req.TopicName, req.GroupName)
	}
	h.Return(writer, nil)
}
func (h *HttpHandler) Return(writer http.ResponseWriter, data any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusBadRequest)
	var successRet = struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data any    `json:"data"`
	}{
		Code: 200,
		Msg:  "success",
		Data: data,
	}
	json.NewEncoder(writer).Encode(successRet)
}

func (h *HttpHandler) FetchTopicDb(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	bytes, err := json.Marshal(h.broker.topicRouteManager.topicDb)
	if err != nil {
		log.Printf("FetchTopicDb error %+v", err)
	} else {
		log.Printf("FetchTopicDb data:%s", string(bytes))
		h.Return(writer, string(bytes))
	}
}

func (h *HttpHandler) FetchTopicData(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	for topicName, t := range h.broker.topicRouteManager.topics {
		log.Printf("主题名称：%s", topicName)
		for _, c := range t.ConsumeQueues {
			log.Printf("队列：%d", c.queueId)
		}
	}
}

func (h *HttpHandler) FetchTopicTable(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	for _, r := range h.broker.topicRouteManager.table {
		log.Printf("%s | %s | %d | %d | %s", r.topic, r.consumerGroup, r.queueId, r.offset, r.clientId)
	}
}

func (h *HttpHandler) ProduceMessage(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
	go func() {
		time.Sleep(5 * time.Second)
		for i := 0; i < 100; i++ {
			rand.Seed(time.Now().UnixNano())
			intn := rand.Intn(2 * 4 * 1024)
			body := make([]byte, intn)
			body = append(body, []byte(fmt.Sprintf("123-%d", i))...)
			message := protocol.Message{
				Topic:          "news",
				Body:           body,
				BornTimestamp:  12312312,
				StoreTimestamp: 0,
				MsgId:          tools.GenerateSerialNumber("P") + "--" + strconv.Itoa(i),
				ProducerGroup:  "123",
				ConsumerGroup:  "123",
			}
			log.Printf("开始写入文件%d----------------------------", i)
			// 写文件
			h.broker.ProducerMessage(&message)
			time.Sleep(1 * time.Second)
		}
	}()
}
