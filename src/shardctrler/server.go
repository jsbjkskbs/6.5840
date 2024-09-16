package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	TJoin  = "Join"
	TLeave = "Leave"
	TMove  = "Move"
	TQuery = "Query"

	ResponserTimeout = 500
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead int32

	ch map[int]*chan Record

	history map[int64]*Record

	configs []Config // indexed by config num
}

type Record struct {
	Term   int
	Index  int64
	Config Config
	Err    Err
}

func (sc *ShardCtrler) appendConfig(config Config) {
	if config.Num > sc.configs[len(sc.configs)-1].Num {
		sc.configs = append(sc.configs, config)
	}
}

type Op struct {
	// Your data here.

	OpType  string
	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
	Num     int

	Index int64
	Clerk int64
}

func (sc *ShardCtrler) execute(op *Op) (result Record) {
	result.Index = op.Index
	switch op.OpType {
	case TJoin:
		// max group's shard => join group & min group
		sc.appendConfig(NewConfig(sc.me, sc.configs, op.Servers))
		result.Err = OK
	case TLeave:
		// leave group's shard => min group
		sc.appendConfig(RemoveGroup(sc.me, sc.configs, op.GIDs))
		result.Err = OK
	case TMove:
		// move shard to group
		sc.appendConfig(MoveShard(sc.me, sc.configs, op.Shard, op.GID))
		result.Err = OK
	case TQuery:
		// query config
		result.Config = QueryConfig(sc.me, sc.configs, op.Num)
		result.Err = OK
	}
	return
}

func (sc *ShardCtrler) submit(op *Op) (result Record) {
	sc.mu.Lock()
	if history, exist := sc.history[op.Clerk]; exist && history.Index == op.Index {
		sc.mu.Unlock()
		return *history
	}
	sc.mu.Unlock()

	commandIndex, commandTerm, isLeader := sc.rf.Start(*op)
	if !isLeader {
		result.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	responser := make(chan Record)
	sc.ch[commandIndex] = &responser
	sc.mu.Unlock()

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
			result.Err = ErrWrongLeader
		}
	}

	sc.mu.Lock()
	delete(sc.ch, commandIndex)
	close(responser)
	sc.mu.Unlock()

	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{
		OpType:  TJoin,
		Index:   args.Index,
		Clerk:   args.Clerk,
		Servers: args.Servers,
	}
	result := sc.submit(op)
	reply.Err = result.Err
	reply.WrongLeader = (result.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{
		OpType: TLeave,
		Index:  args.Index,
		Clerk:  args.Clerk,
		GIDs:   args.GIDs,
	}
	result := sc.submit(op)
	reply.Err = result.Err
	reply.WrongLeader = (result.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{
		OpType: TMove,
		Index:  args.Index,
		Clerk:  args.Clerk,
		Shard:  args.Shard,
		GID:    args.GID,
	}
	result := sc.submit(op)
	reply.Err = result.Err
	reply.WrongLeader = (result.Err == ErrWrongLeader)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		OpType: TQuery,
		Index:  args.Index,
		Clerk:  args.Clerk,
		Num:    args.Num,
	}
	result := sc.submit(op)
	reply.Err = result.Err
	reply.WrongLeader = (result.Err == ErrWrongLeader)
	reply.Config = result.Config
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.history = map[int64]*Record{}
	sc.ch = map[int]*chan Record{}

	go sc.run()
	return sc
}

func (sc *ShardCtrler) run() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)

			result := Record{}
			if record, exist := sc.history[op.Clerk]; exist && record.Index >= op.Index {
				result = *record
			} else {
				result = sc.execute(&op)
				sc.history[op.Clerk] = &result
			}
			if responser, exist := sc.ch[applyMsg.CommandIndex]; exist {
				result.Term = applyMsg.CommandTerm
				*responser <- result
			}
		}
		sc.mu.Unlock()
	}
}
