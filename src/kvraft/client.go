package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	value     string
	state     string
	clerkId   int64
	lastId    int
	requestId int
	rpcNum    int
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
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.lastId = 0
	ck.requestId = 1
	ck.state = "wait"
	ck.value = ""
	ck.rpcNum = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}

	i := ck.lastId
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.rpcNum++
		ck.lastId = i
		ck.state = "wait"
		ck.mu.Unlock()
	}()
	for {
		//go ck.SendGetRequest(key, i%len(ck.servers), ck.rpcNum)
		//for t := 0; t < 200; t++ {
		//	ck.mu.Lock()
		//	if ck.state == "true" {
		//		value := ck.value
		//		ck.mu.Unlock()
		//		return value
		//	} else if ck.state == "false" {
		//		i = (i + 1) % len(ck.servers)
		//		ck.mu.Unlock()
		//		break
		//	}
		//	ck.mu.Unlock()
		//	time.Sleep(time.Millisecond * 10)
		//}
		reply := GetReply{}
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			fmt.Printf("OK:i num is %d, key is %s, return %s\n", i, key, reply.Value)
			return reply.Value
		} else {
			i = (i + 1) % len(ck.servers)
			continue
		}
	}
	return ""
}

func (ck *Clerk) SendGetRequest(key string, server int, rpcNum int) {
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestId: ck.requestId,
	}
	reply := GetReply{}
	fmt.Printf("sendGetReq\n")
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if rpcNum == ck.rpcNum {
		if ok && reply.Err == OK {
			ck.value = reply.Value
			ck.state = "true"
			fmt.Printf("OK:i num is %d, key is %s, return %s\n", server, key, reply.Value)
		} else {
			ck.value = ""
			ck.state = "false"
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
		Key:       key,
		Value:     value,
		ClerkId:   ck.clerkId,
		Op:        op,
		RequestId: ck.requestId,
	}
	i := ck.lastId
	defer func() {
		ck.mu.Lock()
		ck.requestId++
		ck.rpcNum++
		ck.lastId = i
		ck.state = "wait"
		ck.mu.Unlock()
	}()
	for {
		reply := PutAppendReply{}
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			return
		} else {
			i = (i + 1) % len(ck.servers)
			continue
		}
	}
	//for {
	//	go ck.SendAppendPutRequest(key, value, op, i%len(ck.servers), ck.rpcNum)
	//	for t := 0; t < 200; t++ {
	//		ck.mu.Lock()
	//		if ck.state == "true" {
	//			ck.mu.Unlock()
	//			return
	//		} else if ck.state == "false" {
	//			i = (i + 1) % len(ck.servers)
	//			ck.mu.Unlock()
	//			break
	//		}
	//		ck.mu.Unlock()
	//		time.Sleep(time.Millisecond * 10)
	//	}
	//}
}

func (ck *Clerk) SendAppendPutRequest(key string, value string, op string, server int, rpcNum int) {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClerkId:   ck.clerkId,
		Op:        op,
		RequestId: ck.requestId,
	}
	reply := PutAppendReply{}
	ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if rpcNum == ck.rpcNum {
		if ok && reply.Err == OK {
			ck.state = "true"
		} else {
			ck.state = "false"
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
