package tools

import (
	"math/rand"
	"time"
)

func GetRandomValue[V any](m map[int]*V) *V {
	rand.Seed(time.Now().UnixNano())
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return m[keys[rand.Intn(len(keys))]]
}
