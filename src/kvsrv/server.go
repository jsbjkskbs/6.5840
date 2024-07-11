package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data    map[string]string
	oldData map[int64]*Record
}

type Record struct {
	CurrentOp int64
	Legacy    string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	old, ok := kv.oldData[args.Nrand]
	if ok && old.CurrentOp == args.Op {
		return
	}

	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	old, ok := kv.oldData[args.Nrand]
	if ok && old.CurrentOp == args.Op {
		reply.Value = old.Legacy
		return
	}

	if !ok {
		kv.oldData[args.Nrand] = &Record{}
	}
	reply.Value = kv.data[args.Key]
	kv.oldData[args.Nrand].CurrentOp = args.Op
	kv.oldData[args.Nrand].Legacy = reply.Value

	kv.data[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = map[string]string{}
	kv.oldData = map[int64]*Record{}
	return kv
}
