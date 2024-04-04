package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastValue    map[int64]string
	dataStore    map[string]string
	serverChan   map[int]chan ServerApply
	clerkRequest map[int64]int
}

type ServerApply struct {
	Value string
	Term  int
	Ok    bool
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var datastore map[string]string
	var lastValue map[int64]string
	var clerkRequest map[int64]int
	if d.Decode(&datastore) != nil ||
		d.Decode(&lastValue) != nil ||
		d.Decode(&clerkRequest) != nil {
		log.Fatalf("snapshot decode error")
	}

	kv.mu.Lock()
	kv.dataStore = datastore
	kv.lastValue = lastValue
	kv.clerkRequest = clerkRequest
	kv.mu.Unlock()
}

func (kv *KVServer) ListenChannel(ch chan raft.ApplyMsg) {
	for applyMsg := range ch {
		if kv.killed() {
			return
		}
		if applyMsg.CommandValid {
			DPrintf("CommandApply!!!!!!!!!\n")
			kv.CommandApply(applyMsg)
		} else if applyMsg.SnapshotValid {
			DPrintf("SnapshotApply!!!!!!!!\n")
			kv.installSnapshot(applyMsg.Snapshot)
		}
	}
	//for !kv.killed() {
	//	select {
	//	case applyMsg := <-ch:
	//		if applyMsg.CommandValid {
	//			DPrintf("CommandApply!!!!!!!!!\n")
	//			kv.CommandApply(applyMsg)
	//		}
	//	}
	//}
}

//func (kv *KVServer) SnapshotApply(applyMsg raft.ApplyMsg) {
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//
//}

func (kv *KVServer) CommandApply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	apply := ServerApply{}
	if op.OpType == "Get" {
		v, ok := kv.dataStore[op.Key]
		if ok {
			apply = ServerApply{
				Value: v,
				Term:  applyMsg.CommandTerm,
				Ok:    true,
			}
			fmt.Printf("[%d]Get key:%s, %s\n", kv.me, op.Key, v)
		} else {
			apply = ServerApply{
				Value: "",
				Term:  applyMsg.CommandTerm,
				Ok:    false,
			}
			fmt.Printf("[%d]Get key wrong:%s, %s\n", kv.me, op.Key, "")
		}
	} else if op.OpType == "Append" {
		if kv.clerkRequest[op.ClientId] >= op.RequestId {
			return
		}
		kv.clerkRequest[op.ClientId] = op.RequestId
		v, ok := kv.dataStore[op.Key]
		if ok {
			newValue := v + op.Value
			fmt.Printf("[%d]append key:%s, %s\n", kv.me, op.Key, newValue)
			kv.dataStore[op.Key] = newValue
		} else {
			fmt.Printf("[%d]append key:%s, %s\n", kv.me, op.Key, op.Value)
			kv.dataStore[op.Key] = op.Value
		}
		apply = ServerApply{
			Value: kv.dataStore[op.Key],
			Term:  applyMsg.CommandTerm,
			Ok:    true,
		}
	} else if op.OpType == "Put" {
		if kv.clerkRequest[op.ClientId] >= op.RequestId {
			return
		}
		kv.clerkRequest[op.ClientId] = op.RequestId
		kv.dataStore[op.Key] = op.Value
		apply = ServerApply{
			Value: kv.dataStore[op.Key],
			Term:  applyMsg.CommandTerm,
			Ok:    true,
		}
		fmt.Printf("[%d]Put key:%s, %s\n", kv.me, op.Key, op.Value)
	}
	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("kv.rf.GetPersistSize = %d\n", kv.rf.GetPersistSize())
		if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= int(float64(kv.maxraftstate)*0.9) {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			_ = e.Encode(kv.dataStore)
			_ = e.Encode(kv.lastValue)
			_ = e.Encode(kv.clerkRequest)
			kv.rf.Snapshot(applyMsg.CommandIndex, w.Bytes())
			DPrintf("applyMsg.CommandIndex:%d SnapShot", applyMsg.CommandIndex)
		}
		return
	}
	if applyChannel, ok := kv.serverChan[index]; ok {
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.me, applyMsg, index)
		//kv.mu.Unlock()
		applyChannel <- apply
		DPrintf("kv.rf.GetPersistSize = %d\n", kv.rf.GetPersistSize())
		if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= int(float64(kv.maxraftstate)*0.9) {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			_ = e.Encode(kv.dataStore)
			_ = e.Encode(kv.lastValue)
			_ = e.Encode(kv.clerkRequest)
			kv.rf.Snapshot(applyMsg.CommandIndex, w.Bytes())
			DPrintf("applyMsg.CommandIndex:%d SnapShot", applyMsg.CommandIndex)
		}
		//kv.mu.Lock()
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.me, applyMsg, index)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	//kv.mu.Lock()
	//if kv.clerkRequest[args.ClerkId] >= args.RequestId {
	//	fmt.Printf("map is %d, args is %d\n", kv.clerkRequest[args.ClerkId], args.RequestId)
	//	reply.Err = OK
	//	reply.Value = kv.lastValue[args.ClerkId]
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if _, ok := kv.serverChan[index]; !ok {
		kv.serverChan[index] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[index]
	kv.mu.Unlock()

	defer func() {
		fmt.Printf("kvserver[%d]: pre删除通道index: %d\n", kv.me, index)
		kv.mu.Lock()
		fmt.Printf("kvserver[%d]: begin删除通道index: %d\n", kv.me, index)
		delete(kv.serverChan, index)
		DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
		kv.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		DPrintf("kvserver[%d]: GET获取到通知结果,index=[%d],replyMsg.value: %s\n", kv.me, index, replyMsg.Value)
		if term == replyMsg.Term {
			reply.Err = OK
			reply.Value = replyMsg.Value
			kv.mu.Lock()
			kv.lastValue[args.ClerkId] = replyMsg.Value
			if kv.clerkRequest[args.ClerkId] < args.RequestId {
				kv.clerkRequest[args.ClerkId] = args.RequestId
			}
			kv.mu.Unlock()
			fmt.Printf("reply value is %s\n", reply.Value)
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("kvserver[%d]: GET处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}
	//go kv.CloseChan(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	kv.mu.Lock()
	if kv.clerkRequest[args.ClerkId] >= args.RequestId {
		kv.mu.Unlock()
		reply.Err = OK
		return
	}
	kv.mu.Unlock()
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if _, ok := kv.serverChan[index]; !ok {
		kv.serverChan[index] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[index]
	kv.mu.Unlock()

	defer func() {
		fmt.Printf("kvserver[%d]: pre删除通道index: %d\n", kv.me, index)
		kv.mu.Lock()
		fmt.Printf("kvserver[%d]: begin删除通道index: %d\n", kv.me, index)
		delete(kv.serverChan, index)
		DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
		kv.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		DPrintf("kvserver[%d]: PUTAPPEND获取到通知结果,index=[%d],replyMsg: %v\n", kv.me, index, replyMsg)
		if term == replyMsg.Term {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("kvserver[%d]: PUTAPPEND处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}

	//go kv.CloseChan(index)
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	//DPrintf("kvserver[%d]: 开始删除通道index: %d\n", kv.me, index)
	defer kv.mu.Unlock()
	ch, ok := kv.serverChan[index]
	if !ok {
		//若该index没有保存通道,直接结束
		DPrintf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}

	close(ch)
	delete(kv.serverChan, index)
	DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
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
	kv.serverChan = make(map[int]chan ServerApply)
	kv.dataStore = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.clerkRequest = make(map[int64]int)
	kv.lastValue = make(map[int64]string)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.installSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.ListenChannel(kv.applyCh)
	return kv
}
