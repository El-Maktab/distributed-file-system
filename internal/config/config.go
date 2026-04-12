package config

import "time"

const (
	ReplicationFactor  = 3
	TransferBufferSize = 4 * 1024 * 1024
)

var (
	HeartbeatInterval   = 1 * time.Second
	HeartbeatTimeout    = 3 * time.Second
	ReplicationInterval = 10 * time.Second
	RPCDeadline         = 5 * time.Second
	UploadStatusTimeout = 15 * time.Second
	UploadStatusPoll    = 200 * time.Millisecond
)
