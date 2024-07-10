package shardctrler

import (
	"6.824/raft"
	"fmt"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	serverChan   map[int]chan ServerApply
	configs      []Config // indexed by config num
	clerkRequest map[int64]int
}

type Op struct {
	// Your data here.
	OpType    string
	Num       int
	GIDs      []int
	Servers   map[int][]string
	ClientId  int64
	RequestId int
}

type ServerApply struct {
	Config Config
	Term   int
	Ok     bool
}

func (sc *ShardCtrler) ListenChannel(ch chan raft.ApplyMsg) {
	for applyMsg := range ch {
		if applyMsg.CommandValid {
			sc.CommandApply(applyMsg)
		}
	}
}

func (sc *ShardCtrler) CommandApply(applyMsg raft.ApplyMsg) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	apply := ServerApply{}
	if op.OpType == "join" {
		old := sc.configs[len(sc.configs)-1].Groups
		group := make(map[int][]string)
		for k, v := range op.Servers {
			group[k] = v
		}
		for k, v := range old {
			group[k] = v
		}
		cf := Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: group,
		}
		var keys []int
		for key := range group {
			keys = append(keys, key)
		}
		sort.Sort(sort.IntSlice(keys))

		fmt.Printf("num:%d len:%d\n", cf.Num, len(cf.Groups))
		j := 0
		k := 0
		num := 10 / len(group)
		l := 10 % len(group)
		fmt.Printf("%d:", sc.me)
		for _, m := range keys {
			i := 0
			if (l + j) >= len(group) {
				for i <= num {
					cf.Shards[k] = m
					fmt.Printf("%d ", m)
					i++
					k++
				}
			} else {
				for i < num {
					cf.Shards[k] = m
					fmt.Printf("%d ", m)
					i++
					k++
				}
			}
			fmt.Printf("\n")
			j++
		}
		sc.configs = append(sc.configs, cf)
		apply = ServerApply{
			Term: applyMsg.CommandTerm,
			Ok:   true,
		}
	} else if op.OpType == "leave" {
		group := make(map[int][]string)
		for k, v := range sc.configs[len(sc.configs)-1].Groups {
			group[k] = v
		}
		id := op.GIDs

		for _, i := range id {
			delete(group, i)
		}

		cf := Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: group,
		}

		//map排序
		var keys []int
		for key := range group {
			keys = append(keys, key)
		}
		sort.Sort(sort.IntSlice(keys))
		fmt.Printf("num:%d len:%d\n", cf.Num, len(cf.Groups))
		if len(group) == 0 {
			sc.configs = append(sc.configs, cf)
		} else {
			j := 0
			k := 0
			num := 10 / len(group)
			l := 10 % len(group)
			fmt.Printf("%d:", sc.me)
			for _, m := range keys {
				i := 0
				if (l + j) >= len(group) {
					for i <= num {
						cf.Shards[k] = m
						fmt.Printf("%d ", m)
						i++
						k++
					}
				} else {
					for i < num {
						cf.Shards[k] = m
						fmt.Printf("%d ", m)
						i++
						k++
					}
				}
				j++
			}
			fmt.Printf("\n")
			sc.configs = append(sc.configs, cf)
		}
		apply = ServerApply{
			Term: applyMsg.CommandTerm,
			Ok:   true,
		}
	} else if op.OpType == "query" {
		num := 0
		if op.Num == -1 || op.Num >= len(sc.configs) {
			num = len(sc.configs) - 1
		} else {
			num = op.Num
		}
		apply = ServerApply{
			Config: sc.configs[num],
			Term:   applyMsg.CommandTerm,
			Ok:     true,
		}
		//if num >= len(sc.configs) {
		//	apply = ServerApply{
		//		Ok: false,
		//	}
		//} else {
		//	apply = ServerApply{
		//		Config: sc.configs[num],
		//		Term:   applyMsg.CommandTerm,
		//		Ok:     true,
		//	}
		//}
	}

	if _, isLeader := sc.rf.GetState(); !isLeader {
		return
	}
	if applyChannel, ok := sc.serverChan[index]; ok {
		applyChannel <- apply
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.clerkRequest[args.ClerkId] >= args.RequestId {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	op := Op{
		OpType:    "join",
		Servers:   args.Servers,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(op)
	if index == -1 {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if _, ok := sc.serverChan[index]; !ok {
		sc.serverChan[index] = make(chan ServerApply, 1)
	}
	replyChan := sc.serverChan[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.serverChan, index)
		sc.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if term == replyMsg.Term {
			reply.Err = OK
			sc.mu.Lock()
			if sc.clerkRequest[args.ClerkId] < args.RequestId {
				sc.clerkRequest[args.ClerkId] = args.RequestId
			}
			sc.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	fmt.Printf("join\n")
	reply.WrongLeader = false
	return

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if sc.clerkRequest[args.ClerkId] >= args.RequestId {
		sc.mu.Unlock()
		reply.Err = OK
		return
	}
	sc.mu.Unlock()
	op := Op{
		OpType:    "leave",
		GIDs:      args.GIDs,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(op)
	if index == -1 {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if _, ok := sc.serverChan[index]; !ok {
		sc.serverChan[index] = make(chan ServerApply, 1)
	}
	replyChan := sc.serverChan[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.serverChan, index)
		sc.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if term == replyMsg.Term {
			reply.Err = OK
			sc.mu.Lock()
			if sc.clerkRequest[args.ClerkId] < args.RequestId {
				sc.clerkRequest[args.ClerkId] = args.RequestId
			}
			sc.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	fmt.Printf("leave\n")
	reply.WrongLeader = false
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType:    "query",
		Num:       args.Num,
		ClientId:  args.ClerkId,
		RequestId: args.RequestId,
	}
	index, term, _ := sc.rf.Start(op)
	if index == -1 {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	if _, ok := sc.serverChan[index]; !ok {
		sc.serverChan[index] = make(chan ServerApply, 1)
	}
	replyChan := sc.serverChan[index]
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.serverChan, index)
		sc.mu.Unlock()
	}()
	select {
	case replyMsg := <-replyChan:
		//当被通知时,返回结果
		if term == replyMsg.Term {
			reply.Err = OK
			reply.Config = replyMsg.Config
			sc.mu.Lock()
			if sc.clerkRequest[args.ClerkId] < args.RequestId {
				sc.clerkRequest[args.ClerkId] = args.RequestId
			}
			sc.mu.Unlock()
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	reply.WrongLeader = false
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
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
	sc.serverChan = make(map[int]chan ServerApply)
	sc.clerkRequest = make(map[int64]int)
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	go sc.ListenChannel(sc.applyCh)
	return sc
}
