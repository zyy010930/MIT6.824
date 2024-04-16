package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu        sync.Mutex
	clerkId   int64
	requestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clerkId = nrand()
	ck.requestId = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()
	args.Num = num
	args.RequestId = ck.requestId
	args.ClerkId = ck.clerkId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.Err == OK {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()

	args.Servers = make(map[int][]string)
	for k, v := range servers {
		args.Servers[k] = v
	}
	args.RequestId = ck.requestId
	args.ClerkId = ck.clerkId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()
	args.GIDs = gids
	args.RequestId = ck.requestId
	args.ClerkId = ck.clerkId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()
	args.Shard = shard
	args.GID = gid
	args.RequestId = ck.requestId
	args.ClerkId = ck.clerkId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
