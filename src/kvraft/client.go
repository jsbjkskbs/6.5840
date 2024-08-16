package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	id     int64
	index  int64
	leader int
}

const (
	RetryInterval = 100
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) wait() {
	time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
}

func (ck *Clerk) getIndex() int64 {
	ck.index++
	return ck.index - 1
}

func (ck *Clerk) nextLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.id = nrand()
	ck.index = 0
	ck.leader = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:   key,
		Clerk: ck.id,
		Index: ck.getIndex(),
	}

	for {
		reply := GetReply{}
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply); !ok {
			// RPC错误
			reply.Err = ErrRPCFailed
		}

		switch reply.Err {
		case ErrRPCFailed:
			// RPC错误可能是原Leader出现故障
			ck.nextLeader()
			ck.wait()
		case ErrWrongLeader:
			// Leader发生变动
			ck.nextLeader()
			ck.wait()
		case ErrTimeout:
			// 这里不是C/S的网络故障
			// 操作应答超时
			ck.wait()
		case ErrOthers:
			// nothing
		case OK:
			// 正常结果
			return reply.Value
		case ErrNoKey:
			// 正常结果
			return reply.Value
		default:
			DPrintf("what err?: %v", reply.Err)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Clerk: ck.id,
		Index: ck.getIndex(),
	}

	for {
		reply := PutAppendReply{}
		if ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply); !ok {
			// RPC错误
			reply.Err = ErrRPCFailed
		}

		switch reply.Err {
		case ErrRPCFailed:
			// RPC错误可能是原Leader出现故障
			ck.nextLeader()
			ck.wait()
		case ErrWrongLeader:
			// Leader发生变动
			ck.nextLeader()
			ck.wait()
		case ErrTimeout:
			// 这里不是C/S的网络故障
			// 操作应答超时
			ck.wait()
		case ErrOthers:
			// nothing
		case OK:
			// 正常结果
			return
		case ErrNoKey:
			// 正常结果
			return
		default:
			DPrintf("what err?: %v", reply.Err)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
