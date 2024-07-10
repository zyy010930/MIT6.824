package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	fmt.Printf("key2shard %s -> %d\n", key, shard)
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	value     string
	clerkId   int64
	requestId int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.requestId = 1
	ck.value = ""
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {

	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}

	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()

	for {
		//ck.config = ck.sm.Query(-1)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		fmt.Printf("get gid: %d\n", gid)
		for _, i := range ck.config.Shards {
			fmt.Printf("%d ", i)
		}
		fmt.Printf("\n")
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					fmt.Printf("errwronggroup\n")
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		//conf := ck.sm.Query(-1)
		//ck.mu.Lock()
		//ck.config = conf
		//ck.mu.Unlock()
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClerkId:   ck.clerkId,
		Op:        op,
		RequestId: ck.requestId,
	}
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.mu.Unlock()
	}()

	for {
		//ck.config = ck.sm.Query(-1)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		fmt.Printf("put/append gid: %d\n", gid)
		for _, i := range ck.config.Shards {
			fmt.Printf("%d ", i)
		}
		fmt.Printf("\n")
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
		//conf := ck.sm.Query(-1)
		//ck.mu.Lock()
		//ck.config = conf
		//ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
