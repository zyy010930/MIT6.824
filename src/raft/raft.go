package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	leader = iota
	candidate
	follower
)

const (
	AppNormal    = iota // 追加正常
	AppOutOfDate        // 追加过时
	AppKilled           // Raft程序终止
	AppCommitted        // 追加的日志已经提交 (2B
	Mismatch            // 追加不匹配 (2B
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm              int
	votedFor                 int
	state                    int
	heartbeat                time.Duration // 心跳时间
	electionTimeout          time.Duration // 选举时间
	timer                    time.Time     //定时器
	appendTime               string
	log                      []Entry
	commitIndex              int   //已知要提交的最高日志条目的索引（初始化为0，单调递增）
	lastApplied              int   //应用于状态机的最高日志条目的索引（初始化为0，单调递增）
	nextIndex                []int //对于每台服务器，发送到该服务器的下一个日志条目的索引（初始化为leader last log index + 1）
	matchIndex               []int //对于每台服务器，已知要在服务器上复制的最高日志条目的索引（初始化为0，单调增加）
	lastIncludedIndex        int
	lastIncludedTerm         int
	lastAppliedSnapshotIndex int
	snapshot                 []byte
	applyChan                chan ApplyMsg
	applyCond                *sync.Cond
}

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) GetPersistSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.persister.SaveStateAndSnapshot(data, clone(rf.snapshot))
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var log = make([]Entry, 0)
	var currentTerm = 0
	var voteFor = 0
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		fmt.Printf("decode\n")
	} else {
		rf.log = log
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()

	if index <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}
	fmt.Printf("%d:index is %d\n", rf.me, index)
	rf.lastIncludedTerm = rf.log[index-1-rf.lastIncludedIndex].Term
	rf.log = rf.log[index-rf.lastIncludedIndex:]
	if len(rf.log) == 0 {
		rf.log = make([]Entry, 0)
	}
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persist()
	rf.applyCond.Signal()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.mu.Unlock()
}

type InstallSnapshotArgs struct {
	Term              int
	Leader            int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if rf.killed() {
		reply.Term = -1
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("%d: rf.currentTerm=%d,args.Term=%d,args.LastIncludedIndex=%d,args.LastIncludedTerm=%d,rf.lastAppliedSnapshotIndex=%d,len=%d\n", rf.me, rf.currentTerm, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, rf.lastAppliedSnapshotIndex, len(rf.log))
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		reply.Term = -1
		return
	}
	if rf.currentTerm > args.Term || args.LastIncludedIndex < rf.lastAppliedSnapshotIndex {
		reply.Term = rf.currentTerm
		return
	}
	rf.timer = time.Now().Add(time.Duration((600 + rand.Int63n(400)) * int64(time.Millisecond)))
	index := args.LastIncludedIndex - rf.lastIncludedIndex
	tempLog := make([]Entry, 0)
	//tempLog = append(tempLog, Entry{})

	for i := index; i < len(rf.log); i++ {
		tempLog = append(tempLog, rf.log[i])
	}
	rf.log = tempLog
	rf.snapshot = clone(args.Data)
	rf.commitIndex = index
	fmt.Printf("%d: start from install,args = %d,last = %d, len = %d\n", rf.me, args.LastIncludedIndex, rf.lastIncludedIndex, len(rf.log))

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)

	rf.mu.Unlock()
	rf.applyCond.Signal()
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	return

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term == rf.currentTerm {
		fmt.Printf("nextIndex %d change to %d\n", rf.nextIndex[server], rf.lastIncludedIndex+1)
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
		rf.matchIndex[server] = rf.lastIncludedIndex
	}
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := args.Term
	candidateId := args.CandidateId
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.log) > 0 {
		fmt.Printf("%d:投票检查%d,%d,%d,%d,%d,%d\n", rf.me, len(rf.log)+rf.lastIncludedIndex, args.LastLogIndex, rf.log[len(rf.log)-1].Term, args.LastLogTerm, args.Term, rf.currentTerm)
	}

	if term < rf.currentTerm {
		fmt.Printf("%d %s -> %d\n", rf.me, "vote refuse 1", candidateId)
		reply.Term = rf.currentTerm
		rf.persist()
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
	}

	if (len(rf.log) > 0 && ((len(rf.log)+rf.lastIncludedIndex) >= args.LastLogIndex && rf.log[len(rf.log)-1].Term > args.LastLogTerm)) || (len(rf.log) == 0 && rf.lastIncludedIndex != 0 && (rf.lastIncludedIndex >= args.LastLogIndex && rf.lastIncludedTerm > args.LastLogTerm)) {
		rf.currentTerm = args.Term
		rf.persist()
		reply.VoteGranted = false
		return
	} else if (len(rf.log) > 0 && ((len(rf.log)+rf.lastIncludedIndex) > args.LastLogIndex && rf.log[len(rf.log)-1].Term == args.LastLogTerm)) || (len(rf.log) == 0 && rf.lastIncludedIndex != 0 && (rf.lastIncludedIndex > args.LastLogIndex && rf.lastIncludedTerm == args.LastLogTerm)) {
		rf.currentTerm = args.Term
		rf.persist()
		reply.VoteGranted = false
		return
	} else if (len(rf.log) > 0 && ((len(rf.log)+rf.lastIncludedIndex) < args.LastLogIndex && rf.log[len(rf.log)-1].Term > args.LastLogTerm)) || (len(rf.log) == 0 && rf.lastIncludedIndex != 0 && (rf.lastIncludedIndex < args.LastLogIndex && rf.lastIncludedTerm > args.LastLogTerm)) {
		rf.currentTerm = args.Term
		rf.persist()
		reply.VoteGranted = false
		return
	} else {
		reply.Term = args.Term
		if (rf.votedFor == -1 || rf.votedFor == candidateId) && args.Term > rf.currentTerm {
			fmt.Printf("%d %s -> %d\n", rf.me, "vote now", candidateId)
			reply.VoteGranted = true
			rf.votedFor = -1
			rf.state = follower
			rf.currentTerm = args.Term
			rf.timer = time.Now().Add(time.Duration((700 + rand.Int63n(300)) * int64(time.Millisecond)))
			//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
			rf.persist()
			return
		} else {
			fmt.Printf("%d %s -> %d\n", rf.me, "vote refuse 2", candidateId)
			rf.currentTerm = args.Term
			rf.persist()
			reply.VoteGranted = false
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("测试：%d节点term为%d，leader发送term为%d, 接收%d个, PrevLogIndex is %d\n", rf.me, rf.currentTerm, args.Term, len(args.Entries), args.PrevLogIndex)

	if rf.currentTerm == args.Term {
		reply.Term = args.Term
		rf.state = follower
		rf.timer = time.Now().Add(time.Duration((700 + rand.Int63n(300)) * int64(time.Millisecond)))
		//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
	} else if rf.currentTerm < args.Term {
		reply.Term = args.Term
		rf.state = follower
		rf.timer = time.Now().Add(time.Duration((700 + rand.Int63n(300)) * int64(time.Millisecond)))
		//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
		rf.currentTerm = args.Term
		rf.persist()
	} else {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ReplyState = AppOutOfDate
		fmt.Printf("网络分区AppOutOfDate,reply is %d\n", reply.ReplyState)
		return
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		return
	}
	if args.PrevLogIndex > rf.lastIncludedIndex && ((args.PrevLogIndex-rf.lastIncludedIndex) > len(rf.log) || rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term != args.PrevLogTerm) {
		//fmt.Printf("不匹配,prevlogindex = %d,len = %d,lastIndex is %d,term1 = %d,term2 = %d\n", args.PrevLogIndex, len(rf.log), rf.lastIncludedIndex, rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term, args.PrevLogTerm)
		rf.timer = time.Now().Add(time.Duration((700 + rand.Int63n(300)) * int64(time.Millisecond)))
		//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
		fmt.Printf("不匹配,args = %d,lastIndex = %d,len = %d\n", args.PrevLogIndex, rf.lastIncludedIndex, len(rf.log))
		if (args.PrevLogIndex - rf.lastIncludedIndex) <= len(rf.log) {
			fmt.Printf("log.term = %d, lastTerm = %d, args.term = %d\n", rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term, rf.lastIncludedTerm, args.PrevLogTerm)
		}
		reply.Success = false
		reply.ReplyState = Mismatch
		if (args.PrevLogIndex - rf.lastIncludedIndex) > len(rf.log) {
			reply.XTerm = -1
			reply.XLen = len(rf.log) + 1 + rf.lastIncludedIndex
		} else {
			reply.XTerm = rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term
			x := args.PrevLogIndex
			for (x - rf.lastIncludedIndex) > 0 {
				if rf.log[x-1-rf.lastIncludedIndex].Term == reply.XTerm {
					x--
				} else {
					break
				}
			}
			reply.XIndex = x + 1
			fmt.Printf("xindex = %d, last = %d\n", reply.XIndex, rf.lastIncludedIndex)
		}
		return
	}

	record, _ := time.ParseInLocation("Jan _2 15:04:05.000", args.Record, time.Local)
	lastRecord, _ := time.ParseInLocation("Jan _2 15:04:05.000", rf.appendTime, time.Local)
	if record.Before(lastRecord) {
		//fmt.Printf("%s < %s time is over!!!\n", args.Record, rf.appendTime)
		reply.Success = false
		return
	} else {
		rf.appendTime = args.Record
	}

	reply.Success = true
	//rf.timer.Reset(rf.heartbeat)

	//如果存在需要更新的entry则添加到log后面
	if args.Entries != nil {
		//rf.log = rf.log[:args.PrevLogIndex]
		fmt.Printf("updateIndex = %d, lastIndex = %d\n", args.UpdateIndex, rf.lastIncludedIndex)
		rf.log = rf.log[:args.UpdateIndex-rf.lastIncludedIndex]
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		fmt.Printf("%d更新log，长度%d\n", rf.me, len(rf.log))
	}
	reply.Len = len(rf.log) + 1 + rf.lastIncludedIndex

	//for rf.lastApplied < args.LeaderCommit && (rf.lastApplied-rf.lastIncludedIndex) < len(rf.log) {
	//	rf.lastApplied++
	//	applyMsg := ApplyMsg{
	//		CommandValid: true,
	//		Command:      rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command,
	//		CommandIndex: rf.lastApplied,
	//	}
	//	fmt.Printf("follower commit cmd %d, index = %d\n", rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command, applyMsg.CommandIndex)
	//	rf.mu.Unlock()
	//	rf.applyChan <- applyMsg
	//	rf.mu.Lock()
	//	rf.commitIndex = rf.lastApplied
	//}
	//fmt.Printf("%d:apply is %d, leadercommit is %d, len is %d\n", rf.me, rf.lastApplied, args.LeaderCommit, len(rf.log))
	rf.commitIndex = args.LeaderCommit
	rf.applyCond.Signal()

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, num *int) {

	if rf.killed() {
		return
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return
	}
	if reply.ReplyState == AppOutOfDate {
		fmt.Printf("%d %s\n", rf.me, "AppOutOfDate follow -> candidate")
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		//rf.timer = time.Now().Add(time.Duration((900 + rand.Int63n(150)) * int64(time.Millisecond)))
		rf.persist()
		return
	} else if reply.ReplyState == Mismatch {
		//rf.nextIndex[server]--
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
			fmt.Printf("reply XLen is %d\n", reply.XLen)
		} else if reply.XTerm > 0 {
			x := reply.XIndex
			for (x - rf.lastIncludedIndex) <= len(rf.log) {
				if x > rf.lastIncludedIndex && rf.log[x-1-rf.lastIncludedIndex].Term == reply.XTerm {
					x++
				} else {
					break
				}
			}
			rf.nextIndex[server] = x
			fmt.Printf("x is %d, len is %d\n", x, len(rf.log))
		}

		fmt.Printf("%d nextIndex change to %d\n", rf.me, rf.nextIndex[server])
	}

	if reply.Term == rf.currentTerm && reply.Success == true && *num < len(rf.peers)/2 {
		*num++
		fmt.Printf("%d return success! num is %d\n", server, *num)
	}
	fmt.Printf("nextIndex = %d, lastIndex = %d, len = %d\n", rf.nextIndex[server], rf.lastIncludedIndex, len(rf.log))
	if rf.nextIndex[server]-rf.lastIncludedIndex > len(rf.log)+1 {
		return
	}
	if reply.Success && len(args.Entries) != 0 {
		rf.nextIndex[server] = reply.Len
		rf.matchIndex[server] = reply.Len - 1
	}
	if *num >= len(rf.peers)/2 {
		// 保证幂等性，不会提交第二次
		*num = 0

		if len(rf.log)+rf.lastIncludedIndex == 0 {
			return
		}

		max := rf.lastIncludedIndex
		for serveId, _ := range rf.peers {
			if rf.me == serveId {
				continue
			}
			if rf.matchIndex[serveId] > max {
				max = rf.matchIndex[serveId]
			}
		}
		fmt.Printf("1 max is %d\n", max)

		for max > rf.lastIncludedIndex {
			t := 0
			for serveId, _ := range rf.peers {
				if rf.me == serveId {
					//t++
					continue
				}
				if rf.matchIndex[serveId] >= max {
					t++
				}
			}
			if t >= len(rf.peers)/2 {
				break
			}
			max--
		}
		fmt.Printf("2 max is %d\n", max)

		if max == rf.lastIncludedIndex || rf.log[max-1-rf.lastIncludedIndex].Term != rf.currentTerm {
			if max != rf.lastIncludedIndex {
				fmt.Printf("return because max = %d, last = %d, %d != %d\n", max, rf.lastIncludedIndex, rf.log[max-1-rf.lastIncludedIndex].Term, rf.currentTerm)
			}
			return
		}

		//for rf.lastApplied < max {
		//	rf.lastApplied++
		//	applyMsg := ApplyMsg{
		//		CommandValid: true,
		//		Command:      rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command,
		//		CommandIndex: rf.lastApplied,
		//	}
		//	fmt.Printf("commit cmd %d,index = %d\n", rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command, applyMsg.CommandIndex)
		//	rf.commitIndex = rf.lastApplied
		//	rf.mu.Unlock()
		//	rf.applyChan <- applyMsg
		//	rf.mu.Lock()
		//}
		//fmt.Printf("%d:apply is %d, max is %d, commit is %d\n", rf.me, rf.lastApplied, max, rf.commitIndex)

		rf.commitIndex = max
		fmt.Printf("%d:commitIndex -> %d\n", rf.me, rf.commitIndex)
		rf.applyCond.Signal()

	}
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
	UpdateIndex  int
	Record       string
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	UpdateIndex int
	ReplyState  int
	Len         int
	XTerm       int
	XIndex      int
	XLen        int
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != leader {
		return index, term, isLeader
	} else {
		entry := Entry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		rf.persist()
		index = len(rf.log) + rf.lastIncludedIndex
		term = rf.currentTerm
		isLeader = true
	}
	fmt.Printf("---------------\n%d接受新命令%d,term is %d,len is %d\n---------------\n", rf.me, command, term, index)

	//num := 0
	//for serveId, _ := range rf.peers {
	//	if rf.me == serveId {
	//		continue
	//	}
	//	if (rf.nextIndex[serveId] - 1) < rf.lastIncludedIndex {
	//		fmt.Printf("%d %s %d\n", rf.me, "begin to Install", serveId)
	//		args := InstallSnapshotArgs{
	//			Term:              rf.currentTerm,
	//			Leader:            rf.me,
	//			LastIncludedIndex: rf.lastIncludedIndex,
	//			LastIncludedTerm:  rf.lastIncludedTerm,
	//			Offset:            0,
	//			Data:              rf.persister.ReadSnapshot(),
	//			Done:              true,
	//		}
	//		reply := InstallSnapshotReply{
	//			Term: 0,
	//		}
	//		go rf.sendInstallSnapshot(serveId, &args, &reply)
	//	} else {
	//		fmt.Printf("%d %s %d\n", rf.me, "begin to append", serveId)
	//		args := AppendEntriesArgs{
	//			Term:         rf.currentTerm,
	//			LeaderId:     rf.me,
	//			PrevLogIndex: 0,
	//			PrevLogTerm:  0,
	//			UpdateIndex:  0,
	//			LeaderCommit: rf.commitIndex,
	//			Entries:      nil,
	//		}
	//		reply := AppendEntriesReply{
	//			ReplyState: AppNormal,
	//		}
	//
	//		args.Record = time.Now().Format("Jan _2 15:04:05.000")
	//		//fmt.Printf("%s\n", args.Record)
	//		args.PrevLogIndex = rf.nextIndex[serveId] - 1
	//		args.PrevLogTerm = rf.lastIncludedTerm
	//		fmt.Printf("%d: prevlogindex = %d, lastIndex = %d, rf.log = %d\n", rf.me, args.PrevLogIndex, rf.lastIncludedIndex, len(rf.log))
	//		if (args.PrevLogIndex - rf.lastIncludedIndex) > 0 {
	//			args.PrevLogTerm = rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term
	//		}
	//		entries := make([]Entry, 0)
	//		entries = append(entries, rf.log[rf.nextIndex[serveId]-1-rf.lastIncludedIndex:]...)
	//		args.Entries = entries
	//		args.UpdateIndex = rf.nextIndex[serveId] - 1
	//		go rf.sendAppendEntries(serveId, &args, &reply, &num)
	//	}
	//}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader {
			num := 0
			for serveId, _ := range rf.peers {
				if rf.me == serveId {
					continue
				}
				if (rf.nextIndex[serveId] - 1) < rf.lastIncludedIndex {
					fmt.Printf("%d %s %d\n", rf.me, "begin to Install", serveId)
					args := InstallSnapshotArgs{
						Term:              rf.currentTerm,
						Leader:            rf.me,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.lastIncludedTerm,
						Offset:            0,
						Data:              rf.persister.ReadSnapshot(),
						Done:              true,
					}
					reply := InstallSnapshotReply{
						Term: 0,
					}
					go rf.sendInstallSnapshot(serveId, &args, &reply)
				} else {
					fmt.Printf("%d %s %d\n", rf.me, "begin to append", serveId)
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						UpdateIndex:  0,
						LeaderCommit: rf.commitIndex,
						Entries:      nil,
					}
					reply := AppendEntriesReply{
						ReplyState: AppNormal,
					}

					args.Record = time.Now().Format("Jan _2 15:04:05.000")
					//fmt.Printf("%s\n", args.Record)
					args.PrevLogIndex = rf.nextIndex[serveId] - 1
					args.PrevLogTerm = rf.lastIncludedTerm
					fmt.Printf("%d: prevlogindex = %d, lastIndex = %d, rf.log = %d\n", rf.me, args.PrevLogIndex, rf.lastIncludedIndex, len(rf.log))
					if (args.PrevLogIndex - rf.lastIncludedIndex) > 0 {
						args.PrevLogTerm = rf.log[args.PrevLogIndex-1-rf.lastIncludedIndex].Term
					}
					entries := make([]Entry, 0)
					entries = append(entries, rf.log[rf.nextIndex[serveId]-1-rf.lastIncludedIndex:]...)
					args.Entries = entries
					args.UpdateIndex = rf.nextIndex[serveId] - 1
					go rf.sendAppendEntries(serveId, &args, &reply, &num)
				}
			}
		}

		//采用定时器会出现并发读取的bug，故采取此方式
		if time.Now().After(rf.timer) {
			switch rf.state {
			case follower:
				fmt.Printf("%d %s\n", rf.me, "follow -> candidate")
				rf.state = candidate
				fallthrough
			case candidate:
				fmt.Printf("%d %s\n", rf.me, "begin to elect")
				rf.currentTerm += 1
				fmt.Printf("%d term is %d\n", rf.me, rf.currentTerm)
				rf.votedFor = rf.me
				rf.persist()
				Count := 1
				voteNum := 1
				voteRequestArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastIncludedIndex,
					LastLogTerm:  rf.lastIncludedTerm,
				}
				if len(rf.log) > 0 {
					voteRequestArgs.LastLogIndex = len(rf.log) + rf.lastIncludedIndex
					voteRequestArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

				rf.timer = time.Now().Add(time.Duration((600 + rand.Int63n(400)) * int64(time.Millisecond))) //150 -> 900
				//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
				for serveId, _ := range rf.peers {
					if rf.me == serveId {
						continue
					}
					go rf.voteProcess(serveId, &voteRequestArgs, &Count, &voteNum)
				}
				rf.votedFor = -1
				rf.persist()
			}
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) voteProcess(serveId int, args *RequestVoteArgs, count *int, voteNum *int) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serveId, args, &reply)
	for !ok {
		if rf.killed() {
			return
		}
		ok := rf.sendRequestVote(serveId, args, &reply)
		if ok {
			break
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.persist()
		rf.state = follower
		return
	}
	if rf.currentTerm > args.Term {
		fmt.Printf("%d: vote is out of date\n", rf.me)
		return
	}
	*voteNum = *voteNum + 1
	if reply.VoteGranted {
		*count = *count + 1
		fmt.Printf("%d: vote num is %d\n", rf.me, *count)
		if *count > len(rf.peers)/2 && rf.state == candidate && rf.currentTerm == args.Term {
			fmt.Printf("%d is leader\n", rf.me)
			rf.state = leader
			i := 0
			for i < len(rf.peers) {
				rf.nextIndex[i] = len(rf.log) + 1 + rf.lastIncludedIndex //leader选举完成后初始化nextIndex
				i++
			}
			*count = 0

			rf.timer = time.Now().Add(time.Duration((700 + rand.Int63n(300)) * int64(time.Millisecond)))
			//fmt.Printf("now time %s, timer is %s", time.Now(), rf.timer)
			return
		}
	} else {
		return
	}
}

func (rf *Raft) applier(init int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastAppliedSnapshotIndex = init
	for !rf.killed() {
		if rf.lastIncludedIndex > rf.lastAppliedSnapshotIndex {
			applyMsg := ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				Snapshot:      clone(rf.snapshot),
				SnapshotIndex: rf.lastIncludedIndex,
				SnapshotTerm:  rf.lastIncludedTerm,
			}
			rf.lastAppliedSnapshotIndex = rf.lastIncludedIndex
			//if rf.lastApplied < rf.lastIncludedIndex {
			//	rf.lastApplied = rf.lastIncludedIndex
			//}
			rf.lastApplied = rf.lastIncludedIndex
			fmt.Printf("snap [%v][%v][%v] apply index = %v\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastApplied+1)
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
			rf.mu.Lock()
		} else if rf.commitIndex > rf.lastApplied {
			//fmt.Printf("commit\n")
			fmt.Printf("[%v][%v][%v] apply index = %v, commit is %v\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastApplied+1, rf.commitIndex)
			rf.lastApplied += 1
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Command,
				CommandTerm:  rf.log[rf.lastApplied-1-rf.lastIncludedIndex].Term,
			}
			fmt.Printf("%d:{%d}unlock now\n", rf.me, rf.lastApplied)
			rf.mu.Unlock()
			fmt.Printf("%d:{%d}unlock end\n", rf.me, rf.lastApplied)
			rf.applyChan <- msg
			fmt.Printf("%d:{%d}lock now\n", rf.me, rf.lastApplied)
			rf.mu.Lock()
			fmt.Printf("%d:{%d}lock end\n", rf.me, rf.lastApplied)
			//fmt.Printf("%d: commit %d now\n", rf.me, msg.Command)
		} else {
			fmt.Printf("%d:rf.commitIndex = %d, rf.lastApplied = %d\n", rf.me, rf.commitIndex, rf.lastApplied)
			rf.applyCond.Wait()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.state = follower
	rf.votedFor = -1
	rf.heartbeat = 1000 * time.Millisecond
	rf.electionTimeout = time.Duration((150 + rand.Int63n(150)) * int64(time.Millisecond))
	rf.timer = time.Now().Add(rf.electionTimeout)
	rf.appendTime = time.Now().Format("Jan _2 15:04:05.000")
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastAppliedSnapshotIndex = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.snapshot = persister.ReadSnapshot()
	rf.lastAppliedSnapshotIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	fmt.Printf("len=%d,last=%d\n", len(rf.log), rf.lastIncludedIndex)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(rf.lastIncludedIndex)

	return rf
}
