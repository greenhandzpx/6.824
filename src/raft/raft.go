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
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

// Raft
// A Go object implementing a single Raft peer.
//
type Entry struct {
	command interface{}
	term    int // 存该entry所在的term
	index   int // 存该entry在log中的索引
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []Entry

	// volatile state on all servers:
	commitIndex int
	lastApplied int

	// volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// 表明自己是leader、follower还是candidate
	// 0: follower
	// 1: candidate
	// 2: leader
	state           int
	lastTime        int64 // 上次收到leader信息的时间点
	electionTimeout int64 // 单位ms
	leaderId        int   // 表明当前是否有leader
	alives          int   // 作为leader时用来表明有效回应的个数
	votes           int   // 对当前票数进行计数
	//failIds []bool // 用来记录出现网络异常等情况而不能发送rpc的服务器
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
//the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("candidate's term:%v id:%v server's term:%v id:%v votes for %v", args.Term,
		args.CandidateId, rf.currentTerm, rf.me, rf.votedFor)
	//fmt.Println(time.Now().UnixMilli(), "candidate's term:", args.Term, " id:", args.CandidateId, " server's term:", rf.currentTerm,
	//	" id:", rf.me, "votes for", rf.votedFor)
	reply.Term = args.Term

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		// 该follower的term小于candidate的，那可以无脑投(无论是follower还是candidate）
		// 直接降为follower
		rf.state = 0
		rf.lastTime = time.Now().UnixMilli()
		DPrintf("%v votes for %v", rf.me, args.CandidateId)
		//fmt.Println(time.Now().UnixMilli(), rf.me, "votes for ", args.CandidateId)
		rf.currentTerm = reply.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	if rf.votedFor == args.CandidateId {
		// 说明是第二次遍历到这个follower了
		rf.votedFor = -1
	}
	if args.LastLogTerm < rf.log[len(rf.log)-1].term {
		// 取出rf的log中最后一条entry的term
		reply.VoteGranted = false
		return
	}

	// 这里不太确定log数组是从1开始存还是从0开始
	if args.LastLogTerm == rf.log[len(rf.log)-1].term {
		if args.LastLogIndex < rf.log[len(rf.log)-1].index {
			reply.VoteGranted = false
			return
		}
	}
	DPrintf("%v votes for %v", rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.lastTime = time.Now().UnixMilli()
	reply.VoteGranted = true

}

// Start
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term         int
	ApplyGranted bool // 表明心跳是否被接受
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.leaderId != -1 && rf.leaderId != args.LeaderId {
		if rf.currentTerm > args.Term {
			// 说明此时leader断线重连后有新的leader了
			reply.ApplyGranted = false
			return
		}
	}
	rf.votedFor = -1
	if args.Term < rf.currentTerm {
		// leader可以直接降为follower了
		reply.Term = rf.currentTerm
		reply.ApplyGranted = false
		return
	}
	reply.ApplyGranted = true
	rf.currentTerm = args.Term
	rf.state = 0 // 设置为follower
	rf.lastTime = time.Now().UnixMilli()
	rf.leaderId = args.LeaderId

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.ApplyGranted {
			rf.alives++
			return true
		}
		// 说明心跳消息被某个server拒绝了,降为follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
		}
		DPrintf("Id: %v shouldn't be leader.(small line) ", rf.me)
		//fmt.Println(time.Now().UnixMilli(), "Id:", rf.me, " shouldn't be leader.(small line) ")
		rf.state = 0
		rf.votedFor = -1
		return true

	} else {
		DPrintf("Id: %v cannot send apply to Id: %v", rf.me, server)
		return false
	}
	return ok
}

// 成为leader后定期发送心跳消息
func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	rf.mu.Unlock()

	for rf.killed() == false {
		// 每次都需要统计当前还没挂掉的server的个数

		rf.mu.Lock()
		rf.alives = 1
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)

		}

		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.alives <= len(rf.peers)/2 {
			// 说明当前在线的机器不到一半
			DPrintf("Id: %v shouldn't be leader.(big line) ", rf.me)
			//fmt.Println(time.Now().UnixMilli(), "Id:", rf.me, " shouldn't be leader.(big line) ")
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

	}

}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.currentTerm > args.Term {
			// 说明该轮选举已经超时，应当作废
			return true
		}
		if rf.state == 0 {
			// 说明已有leader把自己降为follower
			return true
		}
		if reply.Term > rf.currentTerm {
			// follower的term比自己还新，直接降为follower
			DPrintf("Id: %v shouldn't be candidate", rf.me)
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = 0
			return true
		}
		if reply.VoteGranted {
			rf.votes++
		}
		if rf.votes > len(rf.peers)/2 {
			// 超过半数就成为leader
			rf.state = 2
			rf.votedFor = -1
			rf.votes = 0
			DPrintf("%v becomes leader", rf.me)
			//fmt.Println(time.Now().UnixMilli(), rf.me, "becomes leader")
			go rf.heartbeat()
			return true
		}
	} else {
		DPrintf("term: %v, Id: %v cannot send vote to Id: %v", rf.currentTerm, rf.me, server)
	}
	return ok
}

func (rf *Raft) election() {

	rf.mu.Lock()
	rf.votes = 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].index,
		LastLogTerm:  rf.log[len(rf.log)-1].term,
	}
	rf.mu.Unlock()

	for k := rf.me + 1; k%len(rf.peers) != rf.me%len(rf.peers); k++ {
		// 遍历其他server
		i := k % len(rf.peers)

		// 每次发送rpc前先查看是否已有leader修改了自己的lastTime
		rf.mu.Lock()
		if rf.currentTerm > args.Term {
			// 说明该轮选举已经超时，应当作废
			rf.mu.Unlock()
			return
		}
		if rf.leaderId != -1 || rf.state == 0 {
			//if time.Now().UnixMilli()-rf.lastTime <= rf.electionTimeout {
			// 说明此时已有leader
			DPrintf("Id: %v (small line)There exists a leader.", rf.me)
			//fmt.Println(time.Now().UnixMilli(), "Id:", rf.me, "(small line)There exists a leader.")
			rf.state = 0
			rf.votedFor = -1
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		reply := RequestVoteReply{}

		DPrintf("term: %v, Id: %v start to send vote to Id: %v", rf.currentTerm, rf.me, i)
		//fmt.Println(time.Now().UnixMilli(), "id:", rf.me, "starts to send vote to id:", i)
		go rf.sendRequestVote(i, &args, &reply)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		ms := 30 + rand.Int63()%30
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.state == 2 {
			// 成为leader的话就直接跳过
			rf.mu.Unlock()
			continue
		}
		// 太久没收到心跳消息就进入选举状态
		if time.Now().UnixMilli()-rf.lastTime > rf.electionTimeout {

			DPrintf("Id: %v timeout, start to be candidate", rf.me)
			//fmt.Println(time.Now().UnixMilli(), "timeout ", "me: ", rf.me)
			rf.leaderId = -1
			rf.lastTime = time.Now().UnixMilli()
			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			//for i := 0; i < len(rf.failIds); i++ {
			//	rf.failIds[i] = true
			//}
			rf.mu.Unlock()

			rf.election()

		} else {
			rf.mu.Unlock()
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	// 由于log下标从1开始，所以这里先随便填一个进去（不太确定）
	rf.log = append(rf.log, Entry{
		index: 0,
		term:  0,
	})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastTime = time.Now().UnixMilli()
	rf.state = 0

	// 每个server的timeout在400到800ms之间(待定）
	rf.electionTimeout = 400 + rand.Int63()%400
	rf.leaderId = -1
	rf.votes = 0
	rf.alives = 0

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
