package shardkv

import "time"

const (
	ClerkTimeOut     = 200 * time.Millisecond
	ResponserTimeout = 50 * time.Millisecond
)

const (
	KV = iota
	PullShard
	UpdateConfig
	RemoveShard
)

const (
	Waiting = iota
	Pulling
	Pushing
	Removed
)

const (
	TGet    = "Get"
	TPut    = "Put"
	TAppend = "Append"
)

type ShardCtrlerArgs struct {
	Shard     int
	Num int
}

type ShardCtrlerReply struct {
	Data      []byte
	Err       Err
	Num int
}

type Command struct {
	Type int
	Op   interface{}
}
