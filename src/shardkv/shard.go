package shardkv

import (
	"bytes"
	"time"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

type PullShardOp struct {
	Shard int
	Data  []byte
	Num   int
}

type UpdateConfigOp struct {
	Config shardctrler.Config
}

type RemoveShardOp struct {
	Shard int
	Num   int
}

const (
	CommonInterval = 50 * time.Millisecond
)

func (kv *ShardKV) readShard(shard int, data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// 注意不是 []map[string]string
	// 这是一个分片
	var db map[string]string
	var lastCommandIndex map[int64]int64

	if d.Decode(&db) != nil || d.Decode(&lastCommandIndex) != nil {
	} else {
		kv.db[shard] = db
		kv.lastIndex[shard] = lastCommandIndex
	}
}

func (kv *ShardKV) Shard(shard int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db[shard])
	e.Encode(kv.lastIndex[shard])

	return w.Bytes()
}

func (kv *ShardKV) pullShard(op *PullShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist()

	// !(Config匹配&状态正确)
	if op.Num != kv.currentConfig.Num || kv.status[op.Shard] != Pulling {
		return
	}

	// 读取分片
	kv.readShard(op.Shard, op.Data)

	// 恢复服务
	kv.status[op.Shard] = Waiting
}

func (kv *ShardKV) updateConfig(op *UpdateConfigOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist()

	config := op.Config

	// 日志需要保证是下一个
	// 否则prevConfig无法赋值
	if config.Num != kv.currentConfig.Num+1 {
		return
	}

	// 需要Shard均未被占用才可以更新
	if !kv.isShardUnoccupied() {
		return
	}

	// 更新配置
	kv.prevConfig = kv.currentConfig
	kv.currentConfig = config

	for i := range config.Shards {
		// 这个操作表明prev与current需要遵循严格的相邻关系
		// 分片在先前的节点而不在当前的节点,则需要拉取
		// kv.prevConfig.Shards[i] != kv.gid && kv.currentConfig.Shards[i] == kv.gid
		if kv.isShardNeedPull(i) {
			if !kv.isPrevShardContainData(i) {
				// The very first configuration should be numbered zero.
				// It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
				// 0代表无数据，不需要更改
				kv.status[i] = Waiting
			} else {
				kv.status[i] = Pulling
			}
		}

		// 同理，当分片在当前的节点而不在最新配置(currentConfig)选择的节点,则需要推送
		if kv.isShardNeedPush(i) {
			if !kv.isCurrentShardContainData(i) {
				// 移除分片或者声明为空
				kv.status[i] = Removed
			} else {
				kv.status[i] = Pushing
			}
		}
	}
}

func (kv *ShardKV) removeShard(op *RemoveShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist()

	if kv.currentConfig.Num != op.Num {
		return
	}

	if kv.status[op.Shard] != Pushing {
		return
	}

	// 移除分片
	kv.db[op.Shard] = make(map[string]string)
	kv.lastIndex[op.Shard] = make(map[int64]int64)
	kv.status[op.Shard] = Removed
}

func (kv *ShardKV) isShardNeedPull(shard int) bool {
	return kv.prevConfig.Shards[shard] != kv.gid && kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) isShardNeedPush(shard int) bool {
	return kv.prevConfig.Shards[shard] == kv.gid && kv.currentConfig.Shards[shard] != kv.gid
}

func (kv *ShardKV) isPrevShardContainData(shard int) bool {
	return kv.prevConfig.Shards[shard] != 0
}

func (kv *ShardKV) isCurrentShardContainData(shard int) bool {
	return kv.currentConfig.Shards[shard] != 0
}

func (kv *ShardKV) isShardValid(shard int) bool {
	return kv.status[shard] == Waiting && kv.currentConfig.Shards[shard] == kv.gid
}

func (kv *ShardKV) isShardUnoccupied() bool {
	for _, status := range kv.status {
		// 分片被拉取或是推送都视为占用
		if status == Pulling || status == Pushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			if kv.isShardUnoccupied() {
				config := kv.mck.Query(kv.currentConfig.Num + 1)
				if config.Num == kv.currentConfig.Num+1 {
					kv.rf.Start(Command{Type: UpdateConfig, Op: UpdateConfigOp{Config: config}})
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(CommonInterval)
	}
}

func (kv *ShardKV) shardPuller() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			for i := range kv.status {
				if kv.status[i] == Pulling {
					args := &ShardCtrlerArgs{
						Shard: i,
						Num:   kv.currentConfig.Num,
					}
					go kv.sendPullRequest(kv.prevConfig.Groups[kv.prevConfig.Shards[i]], args)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(CommonInterval)
	}
}

func (kv *ShardKV) sendPullRequest(group []string, args *ShardCtrlerArgs) {
	for _, server := range group {
		reply := &ShardCtrlerReply{}
		ok := kv.make_end(server).Call("ShardKV.PullRequestHandler", args, reply)
		if ok && reply.Err == OK {
			kv.rf.Start(Command{
				Type: PullShard,
				Op: PullShardOp{
					Shard: args.Shard,
					Data:  reply.Data,
					Num:   args.Num,
				},
			})
			return
		}
	}
}

func (kv *ShardKV) PullRequestHandler(args *ShardCtrlerArgs, reply *ShardCtrlerReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	// !(Config匹配&正在推送)
	if kv.currentConfig.Num != args.Num {
		return
	}

	if kv.status[args.Shard] != Pushing {
		return
	}

	reply.Err = OK
	reply.Data = kv.Shard(args.Shard)
}

func (kv *ShardKV) shardRemover() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.mu.Lock()
			for i := range kv.status {
				if kv.status[i] == Pushing {
					args := &ShardCtrlerArgs{
						Shard: i,
						Num:   kv.currentConfig.Num,
					}
					go kv.sendRemoveRequest(kv.currentConfig.Groups[kv.currentConfig.Shards[i]], args)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(CommonInterval)
	}
}

func (kv *ShardKV) sendRemoveRequest(group []string, args *ShardCtrlerArgs) {
	for _, server := range group {
		reply := &ShardCtrlerReply{}
		ok := kv.make_end(server).Call("ShardKV.RemoveRequestHandler", args, reply)
		if ok && reply.Err == OK {
			kv.rf.Start(Command{
				Type: RemoveShard,
				Op: RemoveShardOp{
					Shard: args.Shard,
					Num:   args.Num,
				}})
			return
		}
	}
}

func (kv *ShardKV) RemoveRequestHandler(args *ShardCtrlerArgs, reply *ShardCtrlerReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.isRemoveRequestHandled(args) {
		reply.Err = OK
	}
}

func (kv *ShardKV) isRemoveRequestHandled(args *ShardCtrlerArgs) bool {
	// 过期配置或操作已完成
	return (kv.currentConfig.Num == args.Num && kv.status[args.Shard] == Waiting) ||
		kv.currentConfig.Num > args.Num
}
