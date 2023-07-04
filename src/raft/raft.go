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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	currentTerm     int
	votedFor        int
	state           int
	heartbeat       time.Duration // 心跳时间
	electionTimeout time.Duration // 选举时间
	timer           *time.Timer   //定时器
	log             []Entry
	commitIndex     int   //已知要提交的最高日志条目的索引（初始化为0，单调递增）
	lastApplied     int   //应用于状态机的最高日志条目的索引（初始化为0，单调递增）
	nextIndex       []int //对于每台服务器，发送到该服务器的下一个日志条目的索引（初始化为leader last log index + 1）
	matchIndex      []int //对于每台服务器，已知要在服务器上复制的最高日志条目的索引（初始化为0，单调增加）
	applyChan       chan ApplyMsg
	Time            int
}

type Entry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}
	fmt.Printf("%d term is %d\n", rf.me, term)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
		fmt.Printf("投票检查%d,%d,%d,%d\n", len(rf.log), args.LastLogIndex, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
	}
	if term <= rf.currentTerm {
		fmt.Printf("%d %s\n", rf.me, "vote refuse 1")
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if len(rf.log) > 0 && (len(rf.log) >= args.LastLogIndex && rf.log[len(rf.log)-1].Term > args.LastLogTerm) {
		fmt.Printf("%d,%d,%d,%d\n", len(rf.log), args.LastLogIndex, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
		fmt.Printf("%d %s\n", rf.me, "vote refuse 3")
		reply.VoteGranted = false
		return
	} else if len(rf.log) > 0 && (len(rf.log) > args.LastLogIndex && rf.log[len(rf.log)-1].Term == args.LastLogTerm) {
		fmt.Printf("%d,%d,%d,%d\n", len(rf.log), args.LastLogIndex, rf.log[len(rf.log)-1].Term, args.LastLogTerm)
		fmt.Printf("%d %s\n", rf.me, "vote refuse 4")
		reply.VoteGranted = false
		return
	} else {
		reply.Term = args.Term
		if rf.votedFor == -1 || rf.votedFor == candidateId {
			fmt.Printf("%d %s\n", rf.me, "vote now")
			reply.VoteGranted = true
			rf.votedFor = -1
			rf.state = follower
			rf.currentTerm = args.Term
			rf.timer.Reset(rf.heartbeat)
			return
		} else {
			fmt.Printf("%d %s\n", rf.me, "vote refuse 2")
			rf.currentTerm = args.Term
			reply.VoteGranted = false
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}

	//if rf.Time > args.Time {
	//	reply.Success = false
	//	return
	//} else {
	//	rf.Time = args.Time
	//}
	fmt.Printf("测试%d/%d：%d节点term为%d，leader发送term为%d, 接收%d个, PrevLogIndex is %d\n", args.Time, rf.Time, rf.me, rf.currentTerm, args.Term, len(args.Entries), args.PrevLogIndex)
	i := 0
	for i < len(args.Entries) {
		fmt.Printf("指令%d ", args.Entries[i].Command)
		i++
	}
	fmt.Printf("\n")
	if rf.currentTerm == args.Term {
		reply.Term = args.Term
		rf.state = follower
		rf.timer.Reset(rf.heartbeat)
	} else if rf.currentTerm < args.Term {
		reply.Term = args.Term
		rf.state = follower
		rf.timer.Reset(rf.heartbeat)
		fmt.Printf("节点%d转为follower\n", rf.me)
		rf.currentTerm = args.Term
	} else if len(rf.log) > 0 && rf.log[len(rf.log)-1].Term > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ReplyState = AppOutOfDate
		fmt.Printf("网络分区AppOutOfDate,reply is %d\n", reply.ReplyState)
		return
	} else {
		reply.Success = false
		rf.state = follower
		rf.timer.Reset(rf.heartbeat)
		fmt.Printf("更新节点%d的Term %d -> %d\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		return
	}

	if args.PrevLogIndex > 0 && (args.PrevLogIndex > len(rf.log) || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.ReplyState = Mismatch
		fmt.Printf("不匹配\n")
		return
	}

	if args.PrevLogIndex < rf.lastApplied {
		fmt.Printf("接收index不符合条件\n")
		reply.Success = false
		return
	}

	reply.Success = true
	fmt.Printf("%d recieve %d and PrevLogIndex is %d\n", rf.me, len(args.Entries), args.PrevLogIndex)
	//rf.timer.Reset(rf.heartbeat)

	//如果存在需要更新的entry则添加到log后面
	if args.Entries != nil {
		//rf.log = rf.log[:args.PrevLogIndex]
		rf.log = rf.log[:args.UpdateIndex]
		rf.log = append(rf.log, args.Entries...)
		fmt.Printf("%d更新log，长度%d\n", rf.me, len(rf.log))
	}
	reply.Len = len(rf.log) + 1

	fmt.Printf("第%d个:---%d---%d---\n", rf.me, rf.lastApplied, args.LeaderCommit)
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
		fmt.Printf("%d提交了第%d条命令\n", rf.me, rf.lastApplied)
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, num *int) {
	if rf.killed() {
		return
	}
	fmt.Printf("发送给%d了%d个\n", server, len(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		fmt.Printf("%d kill!!!\n", server)
		if rf.killed() {
			return
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	fmt.Printf("reply state is %d\n", reply.ReplyState)
	if reply.ReplyState == AppOutOfDate {
		fmt.Printf("leader %d转换成follower\n", rf.me)
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.timer.Reset(rf.heartbeat)
		return
	} else if reply.ReplyState == Mismatch {
		rf.nextIndex[server]--
	}

	if ok == false {
		rf.nextIndex[server]--
	} else {
		if reply.Term == rf.currentTerm && reply.Success == true && *num < len(rf.peers)/2 {
			*num++
		}
		if rf.nextIndex[server] > len(rf.log)+1 {
			return
		}
		if reply.Success && len(args.Entries) != 0 {
			fmt.Printf("nextIndex %d变为%d\n", rf.nextIndex[server], reply.Len)
			rf.nextIndex[server] = reply.Len
		}
		fmt.Printf("目前提交了%d个\n", *num)
		if *num >= len(rf.peers)/2 {
			fmt.Printf("num为%d个,len为%d个\n", *num, len(rf.peers))
			// 保证幂等性，不会提交第二次
			*num = 0

			if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term != rf.currentTerm {
				return
			}

			for rf.lastApplied < len(rf.log) {
				rf.lastApplied++
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-1].Command,
					CommandIndex: rf.lastApplied,
				}
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
				fmt.Printf("%d提交了第%d条命令\n", rf.me, rf.lastApplied)
			}

		}
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
	Time         int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	UpdateIndex int
	ReplyState  int
	Len         int
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
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = true
		fmt.Printf("---------------\n%d接受新命令%d\n---------------\n", rf.me, command)
	}

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
	t := 0
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			if rf.killed() {
				fmt.Printf("节点已经断开！！！！！！！！！！！！！！！！！！！！！！！！")
				return
			}
			rf.mu.Lock()
			switch rf.state {
			case leader:
				rf.timer.Reset(rf.heartbeat)
			case follower:
				rf.state = candidate
				fmt.Printf("%d %s\n", rf.me, "follow -> candidate")
				fallthrough
			case candidate:
				fmt.Printf("%d %s\n", rf.me, "begin to elect")
				rf.electionBegin()
			}
			rf.mu.Unlock()
		default:
			//rf.mu.Lock()
			switch rf.state {
			case leader:
				fmt.Printf("%d %s\n", rf.me, "begin to append")
				rf.AppendBegin(t)
				t++
			}
			//rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AppendBegin(t int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timer.Reset(rf.heartbeat)

	num := 0
	for serveId, _ := range rf.peers {
		if rf.me == serveId {
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			UpdateIndex:  0,
			LeaderCommit: rf.commitIndex,
			Entries:      nil,
			Time:         0,
		}
		reply := AppendEntriesReply{
			ReplyState: AppNormal,
		}
		args.PrevLogIndex = rf.nextIndex[serveId] - 1
		args.Time = t
		fmt.Printf("%d号服务器下一日志%d\n", serveId, args.PrevLogIndex)
		for i := range rf.peers {
			fmt.Printf("%d ", rf.nextIndex[i])
			if i == (len(rf.peers) - 1) {
				fmt.Printf("\n")
			}
		}
		fmt.Printf("待发送的日志下标%d,%d,%d\n", rf.nextIndex[0], rf.nextIndex[1], rf.nextIndex[2])
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}
		args.Entries = rf.log[rf.nextIndex[serveId]-1:]
		args.UpdateIndex = rf.nextIndex[serveId] - 1
		fmt.Printf("%d线程创建前，leader的log长度为%d,发送log长度为%d\n", serveId, len(rf.log), len(args.Entries))
		go rf.sendAppendEntries(serveId, &args, &reply, &num)
		//fmt.Printf("发送给%d了%d个\n", serveId, len(args.Entries))
	}
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) electionBegin() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	Count := 1
	voteNum := 1
	voteRequestArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	if len(rf.log) > 0 {
		voteRequestArgs.LastLogIndex = len(rf.log)
		voteRequestArgs.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.electionTimeout = time.Duration((150 + rand.Int63n(150)) * int64(time.Millisecond))
	rf.timer = time.NewTimer(rf.electionTimeout)
	for serveId, _ := range rf.peers {
		if rf.me == serveId {
			continue
		}
		fmt.Printf("%s\n", "voteProcess")
		go rf.voteProcess(serveId, &voteRequestArgs, &Count, &voteNum)
	}
	rf.votedFor = -1
}

func (rf *Raft) voteProcess(serveId int, args *RequestVoteArgs, count *int, voteNum *int) {
	reply := RequestVoteReply{}
	fmt.Printf("%s\n", "sendRequest")
	ok := rf.sendRequestVote(serveId, args, &reply)
	for !ok {
		if rf.killed() {
			return
		}
		fmt.Printf("%s\n", "sendRequest")
		ok := rf.sendRequestVote(serveId, args, &reply)
		if ok {
			break
		}
	}
	fmt.Printf("%s\n", "sendRequest end")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = follower
		fmt.Printf("%s\n", "currentTerm < args.term")
		return
	}
	if rf.currentTerm > args.Term {
		fmt.Printf("%s\n", "currentTerm > args.term")
		return
	}
	*voteNum = *voteNum + 1
	if reply.VoteGranted {
		*count = *count + 1
		fmt.Printf("votecount is %d\n", *count)
		if *count > len(rf.peers)/2 && rf.state == candidate && rf.currentTerm == args.Term {
			fmt.Printf("%d is leader\n", rf.me)
			rf.state = leader
			i := 0
			for i < len(rf.peers) {
				rf.nextIndex[i] = len(rf.log) + 1 //leader选举完成后初始化nextIndex
				i++
			}
			*count = 0
			rf.timer.Reset(rf.heartbeat)
			return
		}
	} else {
		return
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
	fmt.Printf("%s\n", "raft init start")
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
	rf.timer = time.NewTimer(rf.electionTimeout)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.log = make([]Entry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.Time = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Printf("%s\n", "raft init end")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
