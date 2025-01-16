package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       string
	Key          string
	Value        string
	ClientId     int64
	RequestId    int
	Config       shardctrler.Config
	DataStore    map[string]string
	Shard        []int
	Need         map[int]int
	Migrate      map[int]int
	Info         MigrateInfo
	Random       string
	Gid          int
	To           int
	ClerkRequest map[int64]int
	Id           int
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
	dead         int32
	// Your definitions here.
	lastValue    map[int64]string
	dataStore    map[string]string
	serverChan   map[Cid]chan ServerApply
	clerkRequest map[int64]int
	nextCfgNum   int
	cfg          shardctrler.Config
	newCfg       shardctrler.Config
	isMigrate    bool
	migrate      map[int]int
	persister    *raft.Persister
	lastApplied  int
	readyShard   map[int]bool
	infoNum      int
	migrateNum   int
	sendNum      int
	getNum       int
	getEnd       map[int]bool
}

type Cid struct {
	cid   int64
	index int
}

type ServerApply struct {
	Value string
	Term  int
	Ok    bool
	Err   Err
}

type MigrateArgs struct {
	Shard        []int
	DataStore    map[string]string
	Cfg          shardctrler.Config
	ClerkRequest map[int64]int
	Gid          int
}

type MigrateReply struct {
	Ok  bool
	Err Err
}

type MigrateInfo struct {
	Migrate map[int]int
	Need    map[int]int
}

func (kv *ShardKV) ListenChannel(ch chan raft.ApplyMsg) {
	for applyMsg := range ch {
		if applyMsg.CommandValid {
			kv.CommandApply(applyMsg)
		} else if applyMsg.SnapshotValid {
			if applyMsg.Snapshot == nil {
				panic("snapshot can not be nil")
			}
			kv.mu.Lock()
			kv.installSnapshot(applyMsg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) CommandApply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	fmt.Printf("kv.gid=%d, index=%d, last=%d\n", kv.gid, applyMsg.CommandIndex, kv.lastApplied)
	if applyMsg.CommandIndex != kv.lastApplied+1 {
		return
	}
	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	apply := ServerApply{}
	id := Cid{
		cid:   op.ClientId,
		index: index,
	}

	if op.OpType == "Get" {
		if !kv.readyShard[key2shard(op.Key)] {
			fmt.Printf("kv.gid: %d shard:", kv.gid)
			for _, i := range kv.cfg.Shards {
				fmt.Printf("%d ", i)
			}
			fmt.Printf("\n")
			apply = ServerApply{
				Err: ErrWrongGroup,
			}
		} else {
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
		}
	} else if op.OpType == "Append" {
		fmt.Printf("%d apply append %s: %s\n", kv.gid, op.Key, op.Value)
		if kv.clerkRequest[op.ClientId] >= op.RequestId {
			fmt.Printf("[%d-%d] key%s, %s old\n", kv.gid, kv.me, op.Key, op.Value)
			kv.lastApplied = applyMsg.CommandIndex
			apply = ServerApply{
				Err: OK,
				Ok:  true,
			}
		} else if !kv.readyShard[key2shard(op.Key)] {
			fmt.Printf("errgroup key %s: %s\n", op.Key, op.Value)
			apply = ServerApply{
				Err: ErrWrongGroup,
			}
		} else {
			//if kv.clerkRequest[op.ClientId] >= op.RequestId {
			//	kv.lastApplied = applyMsg.CommandIndex
			//	return
			//}
			kv.clerkRequest[op.ClientId] = op.RequestId
			v, ok := kv.dataStore[op.Key]
			if ok {
				newValue := v + op.Value
				fmt.Printf("[%d-%d]append key:%s, %s\n", kv.gid, kv.me, op.Key, newValue)
				kv.dataStore[op.Key] = newValue
			} else {
				fmt.Printf("[%d-%d]append key:%s, %s\n", kv.gid, kv.me, op.Key, op.Value)
				kv.dataStore[op.Key] = op.Value
			}
			apply = ServerApply{
				Value: kv.dataStore[op.Key],
				Term:  applyMsg.CommandTerm,
				Err:   OK,
				Ok:    true,
			}
		}
	} else if op.OpType == "Put" {
		if kv.clerkRequest[op.ClientId] >= op.RequestId {
			kv.lastApplied = applyMsg.CommandIndex
			apply = ServerApply{
				Err: OK,
				Ok:  true,
			}
		} else if !kv.readyShard[key2shard(op.Key)] {
			apply = ServerApply{
				Err: ErrWrongGroup,
			}
		} else {
			//if kv.clerkRequest[op.ClientId] >= op.RequestId {
			//	kv.lastApplied = applyMsg.CommandIndex
			//	return
			//}
			kv.clerkRequest[op.ClientId] = op.RequestId
			kv.dataStore[op.Key] = op.Value
			apply = ServerApply{
				Value: kv.dataStore[op.Key],
				Term:  applyMsg.CommandTerm,
				Err:   OK,
				Ok:    true,
			}
			fmt.Printf("[%d]Put key:%s, %s\n", kv.me, op.Key, op.Value)
		}
	} else if op.OpType == "Update" {
		if kv.nextCfgNum > op.Config.Num {
			fmt.Printf("kv.gid = %d, kv.nextCfgNum = %d, op.num = %d, kv.migrateNum = %d, kv.sendNum = %d\n", kv.gid, kv.nextCfgNum, op.Config.Num, kv.migrateNum, kv.sendNum)
			apply = ServerApply{
				Ok: false,
			}
			fmt.Printf("Update return false %d\n", kv.gid)
		} else {
			//如果收到的num和期待的cfgNum相同，则+1
			//kv.nextCfgNum += 1
			//kv.cfg = op.Config
			kv.newCfg = op.Config
			kv.migrateNum = kv.nextCfgNum - 1
			if len(op.Info.Need) == 0 || op.Config.Num == 1 {
				kv.getNum = op.Config.Num
			}
			if len(op.Info.Migrate) == 0 {
				kv.sendNum = op.Config.Num
			}
			if kv.getNum == kv.nextCfgNum && kv.sendNum == kv.nextCfgNum {
				kv.nextCfgNum += 1
				kv.cfg = kv.newCfg
				kv.migrateNum = -1
				fmt.Printf("round end %d, %d\n", kv.nextCfgNum, kv.gid)
			}
			fmt.Printf("kv.gid = %d, kv.nextCfgNum = %d, string = %s, migrateNum = %d\n", kv.gid, kv.nextCfgNum, op.Random, kv.migrateNum)

			if kv.infoNum < op.Config.Num {
				fmt.Printf("%d-%d, kv.infoNum change %d -> %d\n", kv.gid, kv.me, kv.infoNum, op.Config.Num)
				kv.infoNum = op.Config.Num
				for k, _ := range op.Need {
					kv.readyShard[k] = false
					fmt.Printf("need %d readyShard %d flase, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
				}
				if op.Config.Num == 1 {
					for k, _ := range op.Need {
						kv.readyShard[k] = true
						fmt.Printf("info %d readyShard %d true, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
					}
				}

				for k, _ := range op.Migrate {
					kv.readyShard[k] = false
					fmt.Printf("migrate %d readyShard %d false, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
				}
			}

			apply = ServerApply{
				Ok: true,
			}
			fmt.Printf("Update return true %d\n", kv.gid)
		}
	} else if op.OpType == "Migrate" {
		fmt.Printf("Migrate: gid = %d, migrateNum = %d, getNum = %d, opNum = %d\n", kv.gid, kv.migrateNum, kv.getNum, op.Config.Num)
		if kv.getNum == op.Config.Num {
			apply = ServerApply{
				Ok: true,
			}
		} else if kv.getNum > op.Config.Num || (kv.migrateNum != op.Config.Num && op.Config.Num < kv.newCfg.Num) {
			apply = ServerApply{
				Ok:  false,
				Err: ErrTooOld,
			}
		} else if kv.infoNum < op.Config.Num || op.Config.Num > kv.newCfg.Num {
			fmt.Printf("kv.infoNum = %d, op.config.num = %d, kv.cfg.num = %d\n", kv.infoNum, op.Config.Num, kv.newCfg.Num)
			apply = ServerApply{
				Ok:  false,
				Err: ErrCommit,
			}
		} else {
			_, ok := kv.getEnd[op.Shard[0]]
			if !ok {
				fmt.Printf("op.shard = %d\n", op.Shard)
				//kv.dataStore[string(rune(op.Shard))] = op.DataStore[string(rune(op.Shard))]
				for k, v := range kv.dataStore {
					fmt.Printf("kv:key = %s,value = %s\n", k, v)
				}
				for k, v := range op.DataStore {
					fmt.Printf("op:key = %s,value = %s\n", k, v)
					for _, i := range op.Shard {
						if key2shard(k) == i {
							fmt.Printf("key = %s,value = %s,shard = %d, key2shard = %d\n", k, v, op.Shard, key2shard(k))
							kv.dataStore[k] = v
						}
					}
				}
				for _, v := range op.Shard {
					kv.readyShard[v] = true
					fmt.Printf("%d readyShard %d true, cfg.num = %d, kv.num = %d\n", kv.gid, v, op.Config.Num, kv.newCfg.Num)
				}

				for clerkId, ckIndex := range op.ClerkRequest {
					res, ok := kv.clerkRequest[clerkId]
					if !ok || ckIndex > res {
						kv.clerkRequest[clerkId] = ckIndex
					}
				}
				for _, v := range op.Shard {
					kv.getEnd[v] = true
				}
				kv.mu.Unlock()
				migrateInfo := kv.sendInfo(kv.cfg, op.Config)
				kv.mu.Lock()
				fmt.Printf("compare %d %d\n", len(kv.getEnd), len(migrateInfo.Need))
				if len(kv.getEnd) == len(migrateInfo.Need) {
					kv.getNum = op.Config.Num
					kv.getEnd = make(map[int]bool)
				}

				//kv.getNum = op.Config.Num
				if kv.getNum == kv.nextCfgNum && kv.sendNum == kv.nextCfgNum {
					kv.nextCfgNum += 1
					kv.cfg = kv.newCfg
					kv.migrateNum = -1
					fmt.Printf("round end %d, %d\n", kv.nextCfgNum, kv.gid)
				}
			}
			apply = ServerApply{
				Ok: true,
			}
		}
	} else if op.OpType == "Info" {
		//for k, _ := range op.Need {
		//	kv.readyShard[k] = false
		//}
		//if op.Config.Num == 1 {
		//	for k, _ := range op.Need {
		//		kv.readyShard[k] = true
		//	}
		//}
		//
		//for k, _ := range op.Migrate {
		//	kv.readyShard[k] = false
		//}

		if kv.infoNum < op.Config.Num {
			fmt.Printf("%d-%d, kv.infoNum change %d -> %d\n", kv.gid, kv.me, kv.infoNum, op.Config.Num)
			kv.infoNum = op.Config.Num
			for k, _ := range op.Need {
				kv.readyShard[k] = false
				fmt.Printf("need %d readyShard %d flase, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
			}
			if op.Config.Num == 1 {
				for k, _ := range op.Need {
					kv.readyShard[k] = true
					fmt.Printf("info %d readyShard %d true, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
				}
			}

			for k, _ := range op.Migrate {
				kv.readyShard[k] = false
				fmt.Printf("migrate %d readyShard %d false, cfg.num = %d, kv.num = %d\n", kv.gid, k, op.Config.Num, kv.newCfg.Num)
			}
		}
		apply = ServerApply{
			Ok: true,
		}
	} else if op.OpType == "SendNum" {
		if op.Config.Num == kv.nextCfgNum {
			kv.sendNum = kv.nextCfgNum
			if kv.getNum == kv.nextCfgNum && kv.sendNum == kv.nextCfgNum {
				kv.nextCfgNum += 1
				kv.cfg = kv.newCfg
				kv.migrateNum = -1
				fmt.Printf("round end %d, %d\n", kv.nextCfgNum, kv.gid)
			}
		}
		apply = ServerApply{
			Ok: true,
		}
	}
	kv.lastApplied = applyMsg.CommandIndex

	//if _, isLeader := kv.rf.GetState(); !isLeader {
	//	if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= kv.maxraftstate {
	//		fmt.Printf("gid-me: %d-%d snapshot lastApplied = %d\n", kv.gid, kv.me, kv.lastApplied)
	//		kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshotNoneLock(kv.lastApplied))
	//	}
	//	return
	//}
	fmt.Printf("%d-%d use serverChan %d %d\n", kv.gid, kv.me, index, op.ClientId)
	//id := Cid{
	//	cid:   op.ClientId,
	//	index: index,
	//}
	if applyChannel, ok := kv.serverChan[id]; ok {
		applyChannel <- apply
		if op.OpType == "Update" && apply.Ok == true {
			fmt.Printf("apply update %d\n", kv.gid)
		}
		//if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= kv.maxraftstate {
		//	fmt.Printf("gid-me: %d-%d snapshot lastApplied = %d\n", kv.gid, kv.me, kv.lastApplied)
		//	kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshotNoneLock(kv.lastApplied))
		//}
	} else {
		if op.OpType == "Update" && apply.Ok == true {
			fmt.Printf("Err apply update %d\n", kv.gid)
		}
	}
	if kv.maxraftstate != -1 && kv.rf.GetPersistSize() >= kv.maxraftstate {
		fmt.Printf("gid-me: %d-%d snapshot lastApplied = %d\n", kv.gid, kv.me, kv.lastApplied)
		kv.rf.Snapshot(kv.lastApplied, kv.makeSnapshotNoneLock(kv.lastApplied))
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//kv.mu.Lock()
	id := Cid{
		cid:   args.ClerkId,
		index: index,
	}
	if _, ok := kv.serverChan[id]; !ok {
		kv.serverChan[id] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[id]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.serverChan, id)
		kv.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if replyMsg.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = OK
			reply.Value = replyMsg.Value
			kv.mu.Lock()
			kv.lastValue[args.ClerkId] = replyMsg.Value
			if kv.clerkRequest[args.ClerkId] < args.RequestId {
				kv.clerkRequest[args.ClerkId] = args.RequestId
			}
			kv.mu.Unlock()
			fmt.Printf("reply value is %s\n", reply.Value)
		}
		//else {
		//	reply.Err = ErrWrongLeader
		//}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//if kv.clerkRequest[args.ClerkId] >= args.RequestId {
	//	kv.mu.Unlock()
	//	reply.Err = OK
	//	return
	//}
	//kv.mu.Unlock()
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//kv.mu.Lock()
	id := Cid{
		cid:   args.ClerkId,
		index: index,
	}
	if _, ok := kv.serverChan[id]; !ok {
		kv.serverChan[id] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[id]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.serverChan, id)
		kv.mu.Unlock()
	}()

	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if replyMsg.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
		} else if replyMsg.Err == OK {
			fmt.Printf("%s append chan index = %d\n", args.Value, index)
			reply.Err = OK
		} else {
			reply.Err = ""
		}
		//else {
		//	reply.Err = ErrWrongLeader
		//}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) newConfigUpdate(cfg shardctrler.Config, info MigrateInfo) bool {
	kv.mu.Lock()
	//if kv.nextCfgNum != cfg.Num {
	//	kv.mu.Unlock()
	//	return false
	//}
	if kv.nextCfgNum != cfg.Num {
		kv.mu.Unlock()
		return false
	}
	//else if kv.nextCfgNum < cfg.Num {
	//	kv.nextCfgNum = cfg.Num
	//	kv.mu.Unlock()
	//	return false
	//}
	//kv.mu.Unlock()
	op := Op{
		OpType:  "Update",
		Config:  cfg,
		Info:    info,
		Need:    info.Need,
		Migrate: info.Migrate,
		Random:  randstring(10),
	}
	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		kv.mu.Unlock()
		return false
	}
	fmt.Printf("gid = %d, send update\n", kv.gid)
	//kv.mu.Lock()
	id := Cid{
		cid:   0,
		index: index,
	}
	if _, ok := kv.serverChan[id]; !ok {
		kv.serverChan[id] = make(chan ServerApply, 1)
		fmt.Printf("%d-%d create serverChan %d\n", kv.gid, kv.me, index)
	}
	replyChan := kv.serverChan[id]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.serverChan, id)
		fmt.Printf("%d-%d delete serverChan %d\n", kv.gid, kv.me, index)
		kv.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if replyMsg.Ok {
			fmt.Printf("newConfigUpdate return true %d\n", kv.gid)
			return true
		}
		fmt.Printf("newConfigUpdate return false %d\n", kv.gid)
		return false
		//case <-time.After(1000 * time.Millisecond):
		//	return false
	}
}

func (kv *ShardKV) configDetection() {
	clerk := shardctrler.MakeClerk(kv.ctrlers)
	var cfg shardctrler.Config
	for !kv.killed() {
		// 每隔100毫秒询问一次配置服务器
		time.Sleep(100 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			// 不是leader就返回
			continue
		}
		kv.mu.Lock()
		nextCfgNum := kv.nextCfgNum
		fmt.Printf("query kv.gid=%d, num=%d\n", kv.gid, nextCfgNum)
		kv.mu.Unlock()
		cfg = clerk.Query(nextCfgNum)
		go kv.tryToCommitChange(cfg)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill() // Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) sendInfo(old shardctrler.Config, cfg shardctrler.Config) MigrateInfo {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldCfg := old
	newCfg := cfg
	migrate := make(map[int]int)
	need := make(map[int]int)
	//fmt.Printf("old:")
	//for _, gid := range oldCfg.Shards {
	//	fmt.Printf("%d ", gid)
	//}
	//fmt.Printf("\nnew:")
	//for _, gid := range newCfg.Shards {
	//	fmt.Printf("%d ", gid)
	//}
	//fmt.Printf("\n")
	for i, gid := range oldCfg.Shards {
		if newCfg.Shards[i] != gid && gid == kv.gid {
			migrate[i] = newCfg.Shards[i]
			fmt.Printf("i %d migrate %d len = %d\n", i, migrate[i], len(migrate))
		}
	}
	for i, gid := range oldCfg.Shards {
		if newCfg.Shards[i] == kv.gid && gid != kv.gid {
			need[i] = gid
		}
	}
	// kv.cfg = newCfg
	fmt.Printf("kv.cfg: ")
	for _, i := range kv.cfg.Shards {
		fmt.Printf("%d ", i)
	}
	fmt.Printf("\n")
	return MigrateInfo{Migrate: migrate, Need: need}
}

func (kv *ShardKV) outData(gid int, shard []int, to int, cfg shardctrler.Config) bool {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if kv.gid != gid || isLeader != true || kv.infoNum < cfg.Num { //|| cfg.Num <= kv.sendNum
		kv.mu.Unlock()
		return false
	}
	peers, _ := kv.newCfg.Groups[to]
	newMap := make(map[string]string)
	for k, v := range kv.dataStore {
		newMap[k] = v
	}
	cr := make(map[int64]int)
	for k, v := range kv.clerkRequest {
		cr[k] = v
	}
	args := MigrateArgs{
		Shard:        shard,
		DataStore:    newMap,
		Cfg:          cfg,
		ClerkRequest: cr,
		Gid:          kv.gid,
	}
	kv.mu.Unlock()
	//kv.mu.Lock() //7-8
	//for _, v := range shard {
	//	kv.readyShard[v] = false
	//}
	//kv.mu.Unlock()
	for i := 0; i < len(peers); i = (i + 1) % len(peers) {
		fmt.Printf("%d -> %d send Migrate %d, shard = %d\n", kv.gid, to, i, shard)
		reply := MigrateReply{}
		peer := kv.make_end(peers[i])
		res := peer.Call("ShardKV.Migrate", &args, &reply)
		fmt.Printf("Migrate reply.Err = %s, %d\n", reply.Err, kv.gid)
		if res == true && reply.Ok == true {
			//kv.mu.Lock()
			//kv.sendNum += 1
			//fmt.Printf("sendNum: gi = %d, sendNum = %d\n", kv.gid, kv.sendNum)
			//kv.mu.Unlock()
			//kv.SendNum()
			fmt.Printf("outData reply %d\n", kv.gid)
			return true
			//break
		} else if res == false || reply.Err == ErrCommit || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			if i == len(peers)-1 {
				time.Sleep(time.Millisecond * 100)
			}
			continue
		} else {
			return false
		}
	}
	return false
}

func (kv *ShardKV) tryToCommitChange(cfg shardctrler.Config) {
	var old shardctrler.Config
	var res bool
	migrateInfo := MigrateInfo{}
	for !kv.killed() {
		kv.mu.Lock()
		old = kv.cfg
		kv.mu.Unlock()
		sdCfg := shardctrler.Config{}
		sdCfg.Num = cfg.Num
		sdCfg.Shards = cfg.Shards
		sdCfg.Groups = make(map[int][]string)
		for k, v := range cfg.Groups {
			sdCfg.Groups[k] = v
		}
		migrateInfo = kv.sendInfo(old, sdCfg)

		upCfg := shardctrler.Config{}
		upCfg.Num = cfg.Num
		upCfg.Shards = cfg.Shards
		upCfg.Groups = make(map[int][]string)
		for k, v := range cfg.Groups {
			upCfg.Groups[k] = v
		}
		res = kv.newConfigUpdate(upCfg, migrateInfo)
		if res == false {
			return
		} else {
			break
		}
	}
	kv.mu.Lock()
	//migrate := kv.sendInfo(cfg)
	mp := make(map[int][]int)
	for k, v := range migrateInfo.Migrate {
		mp[v] = append(mp[v], k)
	}
	kv.mu.Unlock()
	n := 0
	fmt.Printf("outmap: %d\n", mp)
	for k, v := range mp {
		fmt.Printf("kv:%d outdata %d %d\n", kv.gid, v, k)
		outCfg := shardctrler.Config{}
		outCfg.Num = cfg.Num
		outCfg.Shards = cfg.Shards
		outCfg.Groups = make(map[int][]string)
		for k, v := range cfg.Groups {
			outCfg.Groups[k] = v
		}
		res = kv.outData(kv.gid, v, k, outCfg)
		//kv.SendDataFunc(kv.gid, v, k)
		if res == true {
			n++
		}
		if n == len(mp) {
			kv.SendNum(outCfg)
		}
	}
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	fmt.Printf("Migrate: gid = %d, migrateNum = %d, cfg.num = %d\n", kv.gid, kv.migrateNum, args.Cfg.Num)
	if kv.migrateNum != args.Cfg.Num && args.Cfg.Num < kv.newCfg.Num {
		reply.Ok = false
		reply.Err = ErrTooOld
		kv.mu.Unlock()
		return
	}
	op := Op{
		OpType:       "Migrate",
		DataStore:    args.DataStore,
		Shard:        args.Shard,
		Config:       args.Cfg,
		ClerkRequest: args.ClerkRequest,
		Id:           args.Gid,
	}
	fmt.Printf("kv = %d, shard = %d\n", kv.gid, args.Shard)
	//kv.mu.Unlock()
	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		reply.Ok = false
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	//kv.mu.Lock()
	id := Cid{
		cid:   0,
		index: index,
	}
	if _, ok := kv.serverChan[id]; !ok {
		kv.serverChan[id] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[id]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.serverChan, id)
		kv.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if replyMsg.Ok {
			reply.Ok = true
			return
		}
		reply.Ok = false
		reply.Err = replyMsg.Err
		return
		//case <-time.After(500 * time.Millisecond):
		//	reply.Ok = false
		//	reply.Err = ErrTimeout
		//	return
	}
}

func (kv *ShardKV) makeSnapshotNoneLock(snapshotIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshotIndex)
	e.Encode(kv.dataStore)
	e.Encode(kv.cfg)
	e.Encode(kv.newCfg)
	e.Encode(kv.nextCfgNum)
	e.Encode(kv.readyShard)
	e.Encode(kv.migrate)
	e.Encode(kv.infoNum)
	e.Encode(kv.migrateNum)
	e.Encode(kv.sendNum)
	e.Encode(kv.getNum)
	e.Encode(kv.clerkRequest)
	e.Encode(kv.getEnd)
	kv.lastApplied = snapshotIndex
	return w.Bytes()
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	fmt.Printf("installSnapshot\n")
	if len(snapshot) == 0 {
		fmt.Printf("len snap = 0\n")
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastApplied int
	var dataStore map[string]string
	var cfg shardctrler.Config
	var newCfg shardctrler.Config
	var nextCfgNum int
	var readyShard map[int]bool
	var migrate map[int]int
	var infoNum int
	var migrateNum int
	var sendNum int
	var getNum int
	var clerkRequest map[int64]int
	var getEnd map[int]bool
	if d.Decode(&lastApplied) != nil ||
		d.Decode(&dataStore) != nil ||
		d.Decode(&cfg) != nil ||
		d.Decode(&newCfg) != nil ||
		d.Decode(&nextCfgNum) != nil ||
		d.Decode(&readyShard) != nil ||
		d.Decode(&migrate) != nil ||
		d.Decode(&infoNum) != nil ||
		d.Decode(&migrateNum) != nil ||
		d.Decode(&sendNum) != nil ||
		d.Decode(&getNum) != nil ||
		d.Decode(&clerkRequest) != nil ||
		d.Decode(&getEnd) != nil {
		log.Fatalf("snapshot decode error")
	}
	if lastApplied < kv.lastApplied {
		fmt.Printf("last= %d, kv.last = %d\n", lastApplied, kv.lastApplied)
		return
	}
	kv.lastApplied = lastApplied
	kv.dataStore = dataStore
	kv.cfg = cfg
	kv.newCfg = newCfg
	kv.nextCfgNum = nextCfgNum
	kv.readyShard = readyShard
	kv.migrate = migrate
	kv.infoNum = infoNum
	kv.migrateNum = migrateNum
	kv.sendNum = sendNum
	kv.getNum = getNum
	kv.clerkRequest = clerkRequest
	kv.getEnd = getEnd
	fmt.Printf("nextCfgNum = %d, cfg.num = %d\n", kv.nextCfgNum, kv.cfg.Num)
	fmt.Printf("datastore: %s\n", kv.dataStore)
}

//func (kv *ShardKV) updateMigrate(need map[int]int, migrate map[int]int, config shardctrler.Config) bool {
//	kv.mu.Lock()
//	op := Op{
//		OpType:  "Info",
//		Need:    need,
//		Migrate: migrate,
//		Config:  config,
//	}
//	//kv.mu.Unlock()
//	fmt.Printf("Test Info\n")
//	index, _, _ := kv.rf.Start(op)
//	if index == -1 {
//		fmt.Printf("False Info\n")
//		kv.mu.Unlock()
//		return false
//	}
//	//kv.mu.Lock()
//	if _, ok := kv.serverChan[index]; !ok {
//		kv.serverChan[index] = make(chan ServerApply, 1)
//	}
//	replyChan := kv.serverChan[index]
//	kv.mu.Unlock()
//
//	defer func() {
//		kv.mu.Lock()
//		delete(kv.serverChan, index)
//		kv.mu.Unlock()
//	}()
//	select {
//	case replyMsg := <-replyChan:
//		//当被通知时,返回结果
//		if replyMsg.Ok {
//			return true
//		}
//		return false
//	case <-time.After(500 * time.Millisecond):
//		return false
//	}
//}

func (kv *ShardKV) SendNum(cfg shardctrler.Config) bool {
	kv.mu.Lock()
	op := Op{
		OpType: "SendNum",
		Config: cfg,
	}
	//kv.mu.Unlock()
	fmt.Printf("Test SendNum %d\n", kv.gid)
	index, _, _ := kv.rf.Start(op)
	if index == -1 {
		fmt.Printf("False SendNum %d\n", kv.gid)
		kv.mu.Unlock()
		return false
	}
	//kv.mu.Lock()
	id := Cid{
		cid:   0,
		index: index,
	}
	if _, ok := kv.serverChan[id]; !ok {
		kv.serverChan[id] = make(chan ServerApply, 1)
	}
	replyChan := kv.serverChan[id]
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.serverChan, id)
		kv.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if replyMsg.Ok {
			return true
		}
		return false
	}
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

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.serverChan = make(map[Cid]chan ServerApply)
	kv.dataStore = make(map[string]string)
	kv.clerkRequest = make(map[int64]int)
	kv.lastValue = make(map[int64]string)
	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.nextCfgNum = 1
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.cfg = shardctrler.Config{}
	kv.cfg.Num = 0
	kv.cfg.Groups = make(map[int][]string)
	kv.newCfg = shardctrler.Config{}
	kv.isMigrate = false
	kv.migrate = make(map[int]int)
	kv.readyShard = make(map[int]bool)
	kv.lastApplied = 0
	kv.infoNum = 0
	kv.migrateNum = -1 //debug
	kv.sendNum = 0
	kv.getNum = 0
	kv.getEnd = make(map[int]bool)
	fmt.Printf("restart: me=%d,gid=%d\n", kv.me, kv.gid)
	kv.installSnapshot(persister.ReadSnapshot())
	go kv.configDetection()
	go kv.ListenChannel(kv.applyCh)
	return kv
}
