package broker

import "time"

const (
	HeartbeatTimeout = 30 * time.Second
	ReBalance        = "reBalance"
	requestIdKey     = "requestId"
)
