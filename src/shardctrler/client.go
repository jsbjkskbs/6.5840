package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	RetryInterval = 100
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	id     int64
	index  int64
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
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
	// Your code here.

	ck.id = nrand()
	ck.index = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, Index: ck.getIndex(), Clerk: ck.id}

	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Query", args, &reply)

		if ok && !reply.WrongLeader && reply.Err == OK {
			return reply.Config
		}

		ck.nextLeader()
		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
		continue
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, Index: ck.getIndex(), Clerk: ck.id}

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Join", args, &reply)

		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}

		ck.nextLeader()
		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
		continue
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, Index: ck.getIndex(), Clerk: ck.id}

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Leave", args, &reply)

		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}

		ck.nextLeader()
		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
		continue
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, Index: ck.getIndex(), Clerk: ck.id}

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardCtrler.Move", args, &reply)

		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}

		ck.nextLeader()
		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
		continue
	}
}
