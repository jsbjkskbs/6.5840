package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key          string
	Value        string
	Type         string
	Clerk        int64
	CommandIndex int64
	Num          int
	Shard        int

	Index int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32

	db [shardctrler.NShards]map[string]string

	// 重复序号、clerk的Get不强求结果与首次发送一致，而Get不论怎么发送都无法影响数据库
	// 因此Get只需要返回当前数据库对应键值即可
	// 除了kvsrv，重复序号、clerk的PutAppend不需要返回值，所以只要对重复的请求发送Err=OK
	// 也就是说——只要能判断是不是重复请求，这完全不需要保存历史信息
	// 所以，之前的kvraft和shardctrler的history确实没用↓
	// history map[int64]*Record
	// 由于存在分片，clerk的请求可能会发送到不同的组
	// 因此应该用数组保存clerk的指令序列数（否则会影响其他分片的判断）
	lastIndex [shardctrler.NShards]map[int64]int64

	// 异步处理
	// 因此为局部非线性化
	// 整体线性化由Raft集群保证
	// kvraft的 commandIndex => chan的逻辑依然可用
	// 因为能够保证 commandIndex 唯一（依然是一个raft集群）
	ch [shardctrler.NShards]map[int]chan Record
	// 当然还可以用单个channel保证完全线性化
	// ch 		chan Record

	// shardctrler
	mck           *shardctrler.Clerk
	prevConfig    shardctrler.Config
	currentConfig shardctrler.Config
	status        [shardctrler.NShards]int

	// 要是Raft.persister大写就好了(Raft.Persister)
	persister   *raft.Persister // for persister.RaftStateSize()
	lastApplied int             // 判断快照/日志是否已应用至状态机
}

type Record struct {
	// 不再需要判断 CommandTerm、OpIndex（OpIndex交由lastIndex保存）
	// Term int // CommandTerm
	Index int    // not OpIndex
	Value string // GetResult
	Err   Err    // OpErr
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if !kv.isShardValid(args.Shard) {
		// 属于该shard的组
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	idx := (kv.me << 16)
	op := Op{
		Shard:        args.Shard,
		Key:          args.Key,
		Type:         TGet,
		Num:          kv.currentConfig.Num,
		Clerk:        args.Clerk,
		CommandIndex: args.Index,

		Index: idx,
	}
	cmd := Command{Type: KV, Op: op}
	kv.mu.Unlock()

	result := kv.submit(cmd)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if !kv.isShardValid(args.Shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	idx := (kv.me << 16)
	op := Op{
		Shard:        args.Shard,
		Key:          args.Key,
		Value:        args.Value,
		Type:         args.Op,
		Num:          kv.currentConfig.Num,
		Clerk:        args.Clerk,
		CommandIndex: args.Index,
		Index:        idx,
	}
	cmd := Command{Type: KV, Op: op}
	kv.mu.Unlock()

	result := kv.submit(cmd)
	reply.Err = result.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PullShardOp{})
	labgob.Register(RemoveShardOp{})
	labgob.Register(UpdateConfigOp{})
	labgob.Register(Record{})
	labgob.Register(Command{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.db = [shardctrler.NShards]map[string]string{}
	kv.ch = [shardctrler.NShards]map[int]chan Record{}
	kv.status = [shardctrler.NShards]int{}
	kv.lastIndex = [shardctrler.NShards]map[int64]int64{}
	for i := range kv.db {
		kv.db[i] = map[string]string{}
		kv.ch[i] = map[int]chan Record{}
		kv.lastIndex[i] = map[int64]int64{}
		kv.status[i] = Removed
	}
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.readPersist(kv.persister.ReadSnapshot())
	go kv.run()
	go kv.configPuller()
	go kv.shardPuller()
	go kv.shardRemover()

	return kv
}

func (kv *ShardKV) persist() {
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	if !kv.enableSnapshot() {
		return
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.lastIndex)
	e.Encode(kv.lastApplied)
	e.Encode(kv.status)
	e.Encode(kv.prevConfig)
	e.Encode(kv.currentConfig)

	data := w.Bytes()
	kv.rf.Snapshot(kv.lastApplied, data)
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	if !kv.enableSnapshot() {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var db [len(kv.db)]map[string]string
	var lastIndex [len(kv.ch)]map[int64]int64
	var lastApplied int
	var status [len(kv.db)]int
	var prevConfig shardctrler.Config
	var currentConfig shardctrler.Config

	if d.Decode(&db) != nil ||
		d.Decode(&lastIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&status) != nil ||
		d.Decode(&prevConfig) != nil ||
		d.Decode(&currentConfig) != nil {
	} else {
		kv.db = db
		kv.lastIndex = lastIndex
		kv.lastApplied = lastApplied
		kv.status = status
		kv.prevConfig = prevConfig
		kv.currentConfig = currentConfig
	}
}

func (kv *ShardKV) run() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		cmd, _ := applyMsg.Command.(Command)
		//if !ok {
		// log.Println("nil <- kv.applyCh")
		//}

		// switch cmd.Op.(type)容易出错
		switch cmd.Type {
		case KV:
			op, _ := cmd.Op.(Op)
			if applyMsg.CommandValid {
				kv.lastApplied = applyMsg.CommandIndex
				kv.execute(&op)
			} else if applyMsg.SnapshotValid {
				kv.readPersist(applyMsg.Snapshot)
			}
		case PullShard:
			op := cmd.Op.(PullShardOp)
			kv.lastApplied = applyMsg.CommandIndex
			kv.pullShard(&op)
		case UpdateConfig:
			op := cmd.Op.(UpdateConfigOp)
			kv.lastApplied = applyMsg.CommandIndex
			kv.updateConfig(&op)
		case RemoveShard:
			op := cmd.Op.(RemoveShardOp)
			kv.lastApplied = applyMsg.CommandIndex
			kv.removeShard(&op)
		}
	}
}

func (kv *ShardKV) submit(cmd Command) (result Record) {
	commandIndex, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}
	op := cmd.Op.(Op)
	kv.mu.Lock()
	if _, exist := kv.ch[op.Shard][commandIndex]; !exist {
		kv.ch[op.Shard][commandIndex] = make(chan Record)
	}
	kv.mu.Unlock()
	responser := kv.ch[op.Shard][commandIndex]

	ticker := time.NewTicker(ClerkTimeOut)
	for {
		select {
		case result = <-responser:
			if result.Index == op.Index {
				return
			}
		case <-ticker.C:
			result.Err = ErrTimeOut
			return
		}
	}
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) execute(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer kv.persist()

	result := Record{}
	result.Index = op.Index

	responser := kv.ch[op.Shard][kv.lastApplied]

	// !(Config匹配&分片可用)
	if op.Num != kv.currentConfig.Num || !kv.isShardValid(op.Shard) {
		result.Err = ErrWrongGroup
		kv.response(responser, result)
		return
	}

	// 防止重复PutAppend
	if op.CommandIndex <= kv.lastIndex[op.Shard][op.Clerk] {
		if op.Type != TGet {
			result.Err = OK
			kv.response(responser, result)
			return
		}
	}

	// 分片带来的索引延后
	if op.CommandIndex > kv.lastIndex[op.Shard][op.Clerk] {
		kv.lastIndex[op.Shard][op.Clerk] = op.CommandIndex
	}

	// PutAppend不需要返回值
	result.Err = OK
	switch op.Type {
	case TGet:
		result.Value = kv.db[op.Shard][op.Key]
		kv.response(responser, result)
	case TPut:
		kv.db[op.Shard][op.Key] = op.Value
		kv.response(responser, result)
	case TAppend:
		kv.db[op.Shard][op.Key] += op.Value
		kv.response(responser, result)
	}
}

func (kv *ShardKV) enableSnapshot() bool {
	return kv.maxraftstate != -1
}

func (kv *ShardKV) response(responser chan Record, data Record) {
	// 分片&重分配导致的任务混乱
	// 不知道为什么传入*chan Record会导致死锁
	if responser == nil {
		return
	}

	ticker := time.NewTicker(ResponserTimeout)
	select {
	case responser <- data:
	case <-ticker.C:
		// 无接收者
	}
}
