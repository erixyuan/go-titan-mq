package core

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateSerialNumber() string {
	// 获取当前日期时间，格式为 YYYYMMDDHHMMSS
	now := time.Now().Format("20060102150405")

	// 可选的其他序列号部分
	// 可以根据需要添加其他部分，例如随机数
	// 这些部分可以用分隔符分隔，以形成一个完整的序列号
	// 例如：20190314123145234-2345
	rand.Seed(time.Now().UnixNano())
	randNum := rand.Intn(100000)
	randNumStr := fmt.Sprintf("%05d", randNum)

	// 组合序列号
	serialNumber := fmt.Sprintf("%s-%s", now, randNumStr)

	return serialNumber
}
