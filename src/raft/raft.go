package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
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
	Command interface{}
	Term    int // 存该entry所在的term
	Index   int // 存该entry在log中的索引
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers:
	currentTerm      int
	votedFor         int
	log              []Entry
	startIndex       int // log里第一个entry的index-1（默认是0）
	lastIncludedTerm int // 上一次打快照时的最后一个entry的term

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

	applyCh chan ApplyMsg

	lastIncludedIndex int // 上一次打快照时的最后一个entry的index

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// 这里不确定要不要存起来
	e.Encode(rf.startIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) int {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return 1
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry
	var startIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		DPrintf("Decode error!")
		return -1
	}
	if err := d.Decode(&votedFor); err != nil {
		DPrintf("Decode error")
		return -1
	}
	if err := d.Decode(&startIndex); err != nil {
		DPrintf("Decode error!")
		return -1
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		DPrintf("Decode error!")
		return -1
	}
	if err := d.Decode(&log); err != nil {
		DPrintf("Decode error!")
		return -1
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.startIndex = startIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.log = log
	return 0
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.startIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
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
// the service says it has created a snapshot that has
// all info up to and including Index. this means the
// service no longer needs the log through (and including)
// that Index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	DPrintf("Calls Snapshot(index:%v) in %v", index, rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludedTerm = rf.log[index-rf.startIndex-1].Term
	rf.lastIncludedIndex = rf.log[index-rf.startIndex-1].Index
	rf.log = rf.log[index-rf.startIndex:]
	rf.startIndex = index
	rf.persist()
	rf.saveStateAndSnapshot(snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("Snapshot outdated(leader %v follower %v)", args.LeaderId, rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.persist()
	}
	applymsg := ApplyMsg{
		CommandValid: false,

		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- applymsg
	rf.lastApplied = args.LastIncludedIndex
	rf.startIndex = args.LastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
	rf.saveStateAndSnapshot(args.Data)

	for i, entry := range rf.log {
		if entry.Index == args.LastIncludedIndex &&
			entry.Term == args.LastIncludedTerm {
			rf.log = rf.log[i+1:]
			return
		}
	}
	rf.log = nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// 过期rpc直接返回
		return ok
	}
	if rf.state != 2 {
		return ok
	}

	if ok {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			DPrintf("%v shouldn't be leader, smaller term than %v", rf.me, server)
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
			return true
		}
		rf.alives++
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex

		return true
	}
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != 2 {
		isLeader = false
	} else {
		entry := Entry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   rf.startIndex + len(rf.log) + 1,
		}
		DPrintf("Entry %v(term %v) is appended to leader %v", entry.Index, entry.Term, rf.me)
		rf.log = append(rf.log, entry)
		rf.persist()
		index = entry.Index
		term = entry.Term
	}

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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("candidate's term:%v id:%v server's term:%v id:%v votes for %v", args.Term,
	//	args.CandidateId, rf.currentTerm, rf.me, rf.votedFor)
	//fmt.Println(time.Now().UnixMilli(), "candidate's term:", args.Term, " id:", args.CandidateId, " server's term:", rf.currentTerm,
	//	" id:", rf.me, "votes for", rf.votedFor)
	reply.Term = args.Term

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// candidate的term大于follower(或者也是candidate)的term
		rf.state = 0
		rf.votedFor = -1
		rf.currentTerm = args.Term
		// 保存状态
		rf.persist()
	}
	// candidate的term大于等于follower的term

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}
	//if rf.votedFor == args.CandidateId {
	//	// 说明是第二次遍历到这个follower了
	//	rf.votedFor = -1
	//}
	if (len(rf.log) == 0 && args.LastLogTerm < rf.lastIncludedTerm) ||
		(len(rf.log) > 0 && args.LastLogTerm < rf.log[len(rf.log)-1].Term) {
		// 取出rf的log中最后一条entry的term
		DPrintf("candidate %v's lastLogTerm is smaller than follower %v", args.CandidateId, rf.me)
		reply.VoteGranted = false
		return
	}

	if (len(rf.log) == 0 && args.LastLogTerm == rf.lastIncludedTerm) ||
		(len(rf.log) > 0 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) {
		// 如果两者的最后一条entry的term相等，则比较log长度
		if len(rf.log)+rf.startIndex > args.LastLogIndex {
			DPrintf("candidate %v's len(log) is smaller than follower %v's", args.CandidateId, rf.me)
			reply.VoteGranted = false
			return
		}
	}

	DPrintf("%v votes for %v", rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.lastTime = time.Now().UnixMilli()
	rf.electionTimeout = 400 + rand.Int63()%300
	reply.VoteGranted = true

}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                  int
	ApplyGranted          bool // 表明心跳是否被接受
	ConflictingTerm       int  // 上一个冲突的entry在follower中的term
	FirstConflictingIndex int  // ConflictingTerm中最早的index
	LogTooShort           int  // 表明log inconsistency的原因是follower的log太短，否则为-1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.FirstConflictingIndex = 0

	if args.Term < rf.currentTerm {
		// leader可以直接降为follower了
		reply.Term = rf.currentTerm
		reply.ApplyGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.state = 0 // 设置为follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
	}
	rf.lastTime = time.Now().UnixMilli()
	rf.electionTimeout = 400 + rand.Int63()%300

	for _, entry := range args.Entries {
		DPrintf("follower %v gets entry %v(term %v)", rf.me, entry.Index, entry.Term)
	}
	//DPrintf("args.PrevLogIndex:%v", args.PrevLogIndex)

	// len(rf.log)+rf.startIndex 代表该server拥有的全部entry
	if len(rf.log)+rf.startIndex < args.PrevLogIndex ||
		(args.PrevLogIndex > rf.startIndex && rf.log[args.PrevLogIndex-1-rf.startIndex].Term != args.PrevLogTerm) {
		// log inconsistency
		DPrintf("%v's last index is %v", rf.me, len(rf.log)+rf.startIndex)
		DPrintf("log inconsistency between leader %v and follower %v", args.LeaderId, rf.me)
		reply.ApplyGranted = false
		if len(rf.log)+rf.startIndex >= args.PrevLogIndex {
			// 查出冲突的最早index
			term := rf.log[args.PrevLogIndex-1-rf.startIndex].Term
			firstIndex := args.PrevLogIndex
			for firstIndex > rf.startIndex && rf.log[firstIndex-1-rf.startIndex].Term == term {
				firstIndex--
			}
			reply.FirstConflictingIndex = firstIndex + 1
			reply.ConflictingTerm = term
			reply.LogTooShort = -1
		} else {
			reply.LogTooShort = len(rf.log) + rf.startIndex
		}
		return
	}
	flag := false

	for _, entry := range args.Entries {
		if entry.Index <= rf.startIndex {
			// args里的entry的index比startIndex还小，直接跳过
			continue
		}
		if len(rf.log) == 0 || rf.log[len(rf.log)-1].Index < entry.Index {
			// args里的entry的index比follower的最后一个entry的index大,则直接加到后面
			rf.log = append(rf.log, entry)
			rf.persist()
			continue
		}
		if rf.log[entry.Index-1-rf.startIndex].Term != entry.Term {
			// 遇到冲突的entry
			flag = true
			DPrintf("Delete entry %v(term %v) in server %v", entry.Index, rf.log[entry.Index-1-rf.startIndex].Term, rf.me)

			rf.log[entry.Index-1-rf.startIndex] = entry
			rf.persist()
		}
	}
	// 删掉follower后面多余的元素
	if len(args.Entries) > 0 && flag == true {
		// 如果flag不为true的话，则多余的entry不用删！！
		rf.log = rf.log[:args.Entries[len(args.Entries)-1].Index-rf.startIndex]
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		//oldCommit := rf.commitIndex
		if len(args.Entries) == 0 || args.LeaderCommit < args.Entries[len(args.Entries)-1].Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.Entries[len(args.Entries)-1].Index
		}

	}
	reply.ApplyGranted = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		//DPrintf("An expired appendEntry reply from %v", server)
		return ok
	}

	if rf.state != 2 {
		return ok
	}

	if ok {
		if reply.ApplyGranted {
			rf.alives++
			if len(args.Entries) == 0 {
				// 说明只是心跳机制
				return true
			}
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index

			return true
		}
		// 说明leader的term比follower的term小，降为follower
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			DPrintf("%v shouldn't be leader, smaller term than %v", rf.me, server)
			//fmt.Println(time.Now().UnixMilli(), "Id:", rf.me, " shouldn't be leader.(small line) ")
			rf.state = 0
			rf.votedFor = -1
			rf.persist()
			return true
		}
		// 说明是log inconsistency
		rf.alives++
		if reply.LogTooShort != -1 {
			DPrintf("follower %v's log too short", server)
			rf.nextIndex[server] = reply.LogTooShort + 1
		} else {
			DPrintf("Back up follower %v", server)
			rf.nextIndex[server] = reply.FirstConflictingIndex

			//term := args.PrevLogTerm
			//index := args.PrevLogIndex
			//// 查看leader是否有follower冲突term的相关entry
			//for term > reply.ConflictingTerm {
			//	term--
			//	index--
			//}
			//if term == reply.ConflictingTerm {
			//	if index < 0 {
			//		rf.nextIndex[server] = 1
			//	} else {
			//		rf.nextIndex[server] = index + 1
			//	}
			//} else {
			//	// leader压根就没有该冲突term的entry
			//	rf.nextIndex[server] = reply.FirstConflictingIndex
			//}
		}
		return true

	} else {
		//DPrintf("Id: %v cannot send apply to Id: %v", rf.me, server)
		return false
	}
}

// 成为leader后定期发送心跳消息
func (rf *Raft) heartbeat() {

	for rf.killed() == false {
		// 每次都需要统计当前还没挂掉的server的个数
		rf.mu.Lock()
		rf.alives = 1
		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.killed() {
				return
			}

			rf.mu.Lock()

			if rf.state != 2 {
				rf.mu.Unlock()
				return
			}

			if rf.startIndex >= rf.nextIndex[i] {
				// 如果nextIndex已经不在log里了
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				rf.mu.Unlock()
				DPrintf("leader %v send snapshot(index:%v) to %v", rf.me, args.LastIncludedIndex, i)
				go rf.sendInstallSnapshot(i, &args, &reply)
				continue
			}

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
			}
			// 把从nextIndex开始到最后的所有entry丢到参数里
			args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]-1-rf.startIndex:]...)
			if len(args.Entries) > 0 {
				if args.Entries[0].Index-rf.startIndex <= 1 {
					// 说明此时不存在上一个entry
					args.PrevLogIndex = rf.startIndex
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					// 参数里的第一个entry的前一个
					args.PrevLogIndex = rf.log[args.Entries[0].Index-2-rf.startIndex].Index
					args.PrevLogTerm = rf.log[args.Entries[0].Index-2-rf.startIndex].Term
				}
			} else {
				if len(rf.log) > 0 {
					args.PrevLogIndex = rf.log[len(rf.log)-1].Index
					args.PrevLogTerm = rf.log[len(rf.log)-1].Term
				} else {
					args.PrevLogIndex = rf.startIndex
					args.PrevLogTerm = rf.lastIncludedTerm
				}
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		}

		time.Sleep(6 * time.Millisecond)

		// 遍历所有有可能可以commit的entry
		rf.mu.Lock()
		if len(rf.log) > 0 {
			for index := rf.log[len(rf.log)-1].Index; index > rf.commitIndex; index-- {
				count := 1
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= index {
						count++
					}
				}
				if count > len(rf.peers)/2 && rf.log[index-1-rf.startIndex].Term == rf.currentTerm {
					// 满足条件的entry即可提交给上层
					// 说明commitIndex可以修改了
					rf.commitIndex = index
					break
				}
			}
		}

		//// 说明当前在线的机器不到一半, 降为follower
		//if rf.alives == 1 {
		//	// 说明当前机器被断网了，成单机了
		//	DPrintf("Id: %v shouldn't be leader.(big line) ", rf.me)
		//	//fmt.Println(time.Now().UnixMilli(), "Id:", rf.me, " shouldn't be leader.(big line) ")
		//	rf.state = 0
		//	rf.votedFor = -1
		//	rf.persist()
		//	rf.mu.Unlock()
		//	return
		//}
		rf.mu.Unlock()
	}

}

func (rf *Raft) sendCommittedEntry() {
	for rf.killed() == false {
		time.Sleep(4 * time.Millisecond)

		rf.mu.Lock()

		if len(rf.log) == 0 {
			rf.mu.Unlock()
			continue
		}

		if rf.lastApplied < rf.startIndex {
			// 这里防止server restart后lastApplied没有更新
			rf.lastApplied = rf.startIndex
		}
		if rf.lastApplied < rf.commitIndex {
			// 先拷贝一份，防止打快照截掉日志
			log := rf.log[rf.lastApplied-rf.startIndex : rf.commitIndex-rf.startIndex]
			rf.mu.Unlock()

			for _, entry := range log {
				rf.mu.Lock()
				if rf.lastApplied >= rf.commitIndex {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
				applymsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.applyCh <- applymsg

				// 这里不太确定要不要在installSnapshot里修改lastApplied(((
				rf.mu.Lock()
				rf.lastApplied = entry.Index
				rf.mu.Unlock()
				DPrintf("Entry %v(term %v) is added to state machine in %v", entry.Index, entry.Term, rf.me)
			}
		} else {
			rf.mu.Unlock()
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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

	if args.Term < rf.currentTerm {
		// 说明该轮选举已经超时，应当作废
		//DPrintf("An expired RequestVote reply from %v", server)
		return ok
	}

	if rf.state != 1 {
		// 自己不再是选举者
		return ok
	}

	if ok {
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
			rf.persist()
			return true
		}
		if reply.VoteGranted {
			rf.votes++
		}
		if rf.votes > len(rf.peers)/2 {
			// 超过半数就成为leader
			rf.state = 2
			rf.votes = 0
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = rf.startIndex + len(rf.log) + 1
				rf.matchIndex[i] = 0
			}
			DPrintf("%v becomes leader", rf.me)
			//fmt.Println(time.Now().UnixMilli(), rf.me, "becomes leader")
			go rf.heartbeat()
			return true
		}
	} else {
		//DPrintf("term: %v, Id: %v cannot send vote to Id: %v", rf.currentTerm, rf.me, server)
	}
	return ok
}

func (rf *Raft) election() {

	rf.mu.Lock()
	rf.votes = 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	args.LastLogIndex = len(rf.log) + rf.startIndex
	if len(rf.log) == 0 {
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		// 遍历其他server
		if i == rf.me {
			continue
		}
		// 每次发送rpc前先查看是否已有leader修改了自己的lastTime
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.currentTerm > args.Term {
			// 说明该轮选举已经超时，应当作废
			rf.mu.Unlock()
			return
		}
		if rf.state == 0 {
			// 说明此时已有leader
			DPrintf("Id: %v (small line)There exists a leader.", rf.me)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		reply := RequestVoteReply{}

		//fmt.Println(time.Now().UnixMilli(), "id:", rf.me, "starts to send vote to id:", i)
		go rf.sendRequestVote(i, &args, &reply)

	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//ms := 31
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		time.Sleep(5 * time.Millisecond)

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
			// 每次tick过后reset一下timeout
			rf.electionTimeout = 400 + rand.Int63()%400
			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.persist()
			//for i := 1; i < len(rf.failIds); i++ {
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

	// Your initialization code here (3A, 2B, 2C).
	if rf.readPersist(rf.persister.ReadRaftState()) == 2 {
		// 说明没有存放数据
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.startIndex = 0
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = -1
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//// 由于log下标从2开始，所以这里先随便填一个进去（不太确定）
	//rf.log = append(rf.log, Entry{
	//	Index: 1,
	//	Term:  1,
	//})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastTime = time.Now().UnixMilli()
	rf.state = 0

	// 每个server的timeout在401到800ms之间(待定）
	rf.electionTimeout = 400 + rand.Int63()%300
	rf.leaderId = -1
	rf.votes = 0
	rf.alives = 0

	rf.applyCh = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.sendCommittedEntry()

	return rf
}
