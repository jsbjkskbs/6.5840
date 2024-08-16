package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	TGet    = "Get"
	TPut    = "Put"
	TAppend = "Append"

	ResponserTimeout = 500
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string
	Clerk int64
	Index int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      map[string]string // 实时数据
	history map[int64]*Record // 历史操作结果

	// 异步处理
	// 因此为局部非线性化
	// 整体线性化由Raft集群保证
	ch map[int]*chan Record
	// 当然还可以用单个channel保证完全线性化
	// ch 		chan Record

	// 要是Raft.persister大写就好了(Raft.Persister)
	persister   *raft.Persister // for persister.RaftStateSize()
	lastApplied int             // 判断快照/日志是否已应用至状态机
}

type Record struct {
	Term  int    // CommandTerm
	Index int64  // OpIndex (not CommandIndex)
	Value string // OpResult
	Err   Err    // OpErr
}

// 类似于数据库的事务处理
func (kv *KVServer) execute(op *Op) (result Record) {
	result.Index = op.Index
	switch op.Type {
	case TGet:
		if v, exist := kv.db[op.Key]; exist {
			result.Err = OK
			result.Value = v
		} else {
			result.Err = ErrNoKey
			result.Value = ""
		}
	case TPut:
		result.Err = OK
		kv.db[op.Key] = op.Value
	case TAppend:
		result.Err = OK
		kv.db[op.Key] += op.Value
	}
	return
}

// 类似于数据库的提交事务
func (kv *KVServer) submit(op *Op) (result Record) {
	kv.mu.Lock()
	// 存在对应的历史记录，则认为是重复操作
	if history, exist := kv.history[op.Clerk]; exist && history.Index >= op.Index {
		kv.mu.Unlock()
		return *history
	}
	kv.mu.Unlock()

	// Raft集群的任何操作均默认占用较长时间
	// 因此需要释放锁
	commandIndex, commandTerm, isLeader := kv.rf.Start(*op)
	if !isLeader {
		result.Err = ErrWrongLeader
		result.Value = ""
		return
	}

	// 创建事务（作为返回值的接收者）
	kv.mu.Lock()
	responser := make(chan Record)
	kv.ch[commandIndex] = &responser
	kv.mu.Unlock()

	// 等待 Raft.commit()提交后 KVServer.run()应答，这是需要时间的，且必须释放锁
	ticker := time.NewTicker(time.Duration(ResponserTimeout) * time.Millisecond)
	select {
	case <-ticker.C:
		result.Err = ErrTimeout
	case msg, ok := <-responser:
		if ok && msg.Term == commandTerm {
			result = msg
		} else if !ok {
			result.Err = ErrOthers
		} else {
			// ok && msg.Term != commandTerm
			result.Err = ErrWrongLeader
			result.Value = ""
		}
	}

	// 事务超时/被应答，则认为完成事务
	kv.mu.Lock()
	delete(kv.ch, commandIndex)
	close(responser)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Key:   args.Key,
		Type:  TGet,
		Clerk: args.Clerk,
		Index: args.Index,
	}

	result := kv.submit(&op)
	reply.Err = result.Err
	reply.Value = result.Value
}

// 感觉很迷惑，既然Client调用PutAppend，那为什么这里没有PutAppend但有Put、Append??
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	switch args.Op {
	case "Put":
		kv.Put(args, reply)
	case "Append":
		kv.Append(args, reply)
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Type:  TPut,
		Clerk: args.Clerk,
		Index: args.Index,
	}

	result := kv.submit(&op)
	reply.Err = result.Err
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Type:  TAppend,
		Clerk: args.Clerk,
		Index: args.Index,
	}

	result := kv.submit(&op)
	reply.Err = result.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.db = map[string]string{}
	kv.history = map[int64]*Record{}
	kv.ch = map[int]*chan Record{}

	kv.readSnapshot(persister.ReadSnapshot())

	go kv.run()

	return kv
}

// 这并不类似于Raft.Snapshot()<-这是方法;
// 其作用是生成快照，相当于Raft.snapshot<-这是成员
func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.history)

	return w.Bytes()
}

// KVServer.readSnapshot != Raft.readSnapshot();
// snapshot -> db;
// snapshot -> history
func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var history map[int64]*Record
	if d.Decode(&db) != nil ||
		d.Decode(&history) != nil {
		// err
	} else {
		kv.db = db
		kv.history = history
	}
}

func (kv *KVServer) run() {
	for !kv.killed() {
		// KVServer.submit() => Raft.commit()
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.CommandValid && !kv.isLogApplied(&applyMsg) {
			// 新日志
			// 理论上Command只有Op类型
			op := applyMsg.Command.(Op)
			// Raft集群保证 kv.lastApplied < applyMsg.CommandIndex
			kv.lastApplied = applyMsg.CommandIndex
			result := Record{}

			if record, exist := kv.history[op.Clerk]; exist && record.Index >= op.Index {
				// 重复操作
				// 一般只会有record.Index == op.Index
				// 一般也不会执行这个部分
				// 因为KVServer.submit()直接返回重复日志
				// 当然也有一种可能，KVServer.submit()短时间内重复发生多次，于是提交多份该时刻认为是新内容的日志
				result = *record
				DPrintf("[NOT ERROR]outdated content have commit to raft cluster")
			} else {
				// 新操作
				result = kv.execute(&op)
				kv.history[op.Clerk] = &result
			}

			// 获取响应通道
			// 应当保证响应通道只能被至多一个接收者和一个发送者拥有
			// 尤其是发送者
			// 上个大锁就能简单保证
			if responser, exist := kv.ch[applyMsg.CommandIndex]; exist {
				result.Term = applyMsg.CommandTerm
				*responser <- result
			}

			// 达到日志容限
			if kv.enableSnapshot() && kv.reachRaftStateLimit() {
				kv.rf.Snapshot(applyMsg.CommandIndex, kv.Snapshot())
			}
		} else if applyMsg.SnapshotValid && !kv.isSnapshotApplied(&applyMsg) {
			// 新快照
			kv.readSnapshot(applyMsg.Snapshot)
			kv.lastApplied = applyMsg.SnapshotIndex
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) reachRaftStateLimit() bool {
	return kv.persister.RaftStateSize() >= kv.maxraftstate*10/9
}

func (kv *KVServer) enableSnapshot() bool {
	return kv.maxraftstate != -1
}

func (kv *KVServer) isLogApplied(msg *raft.ApplyMsg) bool {
	return msg.CommandIndex <= kv.lastApplied
}

func (kv *KVServer) isSnapshotApplied(msg *raft.ApplyMsg) bool {
	return msg.SnapshotIndex < kv.lastApplied
}
