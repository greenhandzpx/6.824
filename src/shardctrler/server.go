package shardctrler

import (
	"6.824/raft"
	"bytes"
	"log"
	"sort"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false
// const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	recvTimeout = 500
	sendTimeout = 100
)

const (
	JOIN  = 0
	LEAVE = 1
	MOVE  = 2
	QUERY = 3
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	dead int32
	// Your data here.

	configs []Config // indexed by config num

	replyChs map[int]chan OpReply

	// the uuid of every client -> request cnt
	ids map[int64]int

	lastApplied int
}

type Op struct {
	// Common info
	Type  int
	Uuid  int64
	Count int
	Term  int
	// Different operations have different args:
	// Join
	Servers map[int][]string // new GID -> servers mappings
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int // desired config number
}

type OpReply struct {
	Type int
	// Query
	Conf        Config
	WrongLeader bool
}

func (sc *ShardCtrler) sendReply(commandIndex int, reply *OpReply) {
	replyCh, ok := sc.replyChs[commandIndex]
	if !ok {
		return
	}
	// only leader whose entry's term is the right term should it notify the client
	if _, isLeader := sc.rf.GetState(); !isLeader {
		return
	}
	go func() {
		select {
		//case op.ReplyCh <- reply:
		case replyCh <- *reply:
			DPrintf("%v send a reply, %v", sc.me, commandIndex)
			if reply.Type == QUERY {
				DPrintf("query reply: group size:%v", len(reply.Conf.Groups))
			}
			//kv.mu.Lock()
			//kv.lastApplied = commandIndex
			//kv.mu.Unlock()
		case <-time.After(sendTimeout * time.Millisecond):
			DPrintf("%v send a reply %v timeout", sc.me, commandIndex)
			return
		}
	}()
}

func (sc *ShardCtrler) handleJoin(op Op, commandIndex int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply := OpReply{
		Type:        JOIN,
		WrongLeader: false,
	}

	if commandIndex <= sc.lastApplied {
		DPrintf("outdated command in %v", sc.me)
		return
	}

	if _, exists := sc.ids[op.Uuid]; exists && sc.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
	} else {
		sc.ids[op.Uuid] = op.Count
		oldConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    oldConfig.Num + 1,
			Groups: make(map[int][]string),
			//Shards: [NShards]int,
		}
		DPrintf("%v handle join req, old conf %v", sc.me, oldConfig.Num)
		// copy the newly added groups
		for k, v := range op.Servers {
			newConfig.Groups[k] = v
		}

		// the num of shards that the newly joined group should own
		shard := NShards / (len(newConfig.Groups) + len(oldConfig.Groups))
		remaining := NShards % (len(newConfig.Groups) + len(oldConfig.Groups))

		if len(oldConfig.Groups) == 0 {
			DPrintf("no groups in %v, new groups size:%v",
				sc.me, len(newConfig.Groups))
			// the first config
			// there isn't any group before
			/**
			 *  s+1 | s+1 | s+1 | s | s | s | s
			 *  |---remaining---|
			 */
			shardIdx := 0
			cnt := 0
			var keys []int
			for k := range newConfig.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, GID := range keys {
				step := shard
				if cnt < remaining {
					step++
					cnt++
				}
				bound := shardIdx + step
				for idx := shardIdx; idx < bound; idx++ {
					newConfig.Shards[idx] = GID
				}
				shardIdx = bound
			}
			sc.configs = append(sc.configs, newConfig)

			sc.lastApplied = commandIndex
			sc.sendReply(commandIndex, &reply)
			return
		}

		// calculate the cnt of shards that every group owns
		shardCnt := make(map[int][]int)
		for i := 0; i < NShards; i++ {
			shardCnt[oldConfig.Shards[i]] = append(shardCnt[oldConfig.Shards[i]], i)
		}
		for GID := range op.Servers {
			shardCnt[GID] = []int{}
		}
		for g, s := range shardCnt {
			DPrintf("idx %v g:%v, s size:%v", commandIndex, g, len(s))
		}
		// fetch all the remaining shards
		var remainingShards []int
		var keys []int
		for k := range shardCnt {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, GID := range keys {
			shards := shardCnt[GID]
			if len(shards) <= shard {
				continue
			}
			DPrintf("idx %v len(shards):%v, shard:%v", commandIndex, len(shards), shard)
			limit := shard
			remainingShards = append(remainingShards, shards[limit:]...)
			shardCnt[GID] = shards[0:limit]
		}
		DPrintf("remainShards size:%v", len(remainingShards))
		// when we fetch all the remaining shards, we can allocate them
		// to those needing shards
		cnt := 0
		startIdx := 0
		for _, GID := range keys {
			shards := shardCnt[GID]
			expect := shard
			if cnt < remaining {
				cnt++
				expect++
			}
			bound := startIdx + expect - len(shards)
			shardCnt[GID] = append(shardCnt[GID],
				remainingShards[startIdx:bound]...)
			startIdx = bound
		}

		// copy the old groups
		for k, v := range oldConfig.Groups {
			newConfig.Groups[k] = v
		}
		// update the allocation of shards
		for group, shards := range shardCnt {
			for _, s := range shards {
				newConfig.Shards[s] = group
			}
		}
		sc.configs = append(sc.configs, newConfig)
		//for s, g := range newConfig.Shards {
		//	DPrintf("s:%v g:%v", s, g)
		//}
		//for s, g := range newConfig.Shards {
		//	DPrintf("s:%v g:%v", s, g)
		//}
	}

	sc.lastApplied = commandIndex
	sc.sendReply(commandIndex, &reply)

}

func (sc *ShardCtrler) handleLeave(op Op, commandIndex int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply := OpReply{
		Type:        LEAVE,
		WrongLeader: false,
	}

	if commandIndex <= sc.lastApplied {
		DPrintf("outdated command in %v", sc.me)
		return
	}

	if _, exists := sc.ids[op.Uuid]; exists && sc.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)

	} else {

		sc.ids[op.Uuid] = op.Count

		oldConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    oldConfig.Num + 1,
			Groups: make(map[int][]string),
			//Shards: [NShards]int,
		}
		// copy all the groups
		for k, v := range oldConfig.Groups {
			newConfig.Groups[k] = v
		}
		// delete the given groups
		for _, GID := range op.GIDs {
			delete(newConfig.Groups, GID)
		}
		// caculate the cnt of shards that should be released
		var remainingShards []int
		// calculate the cnt of shards that every group owns
		shardCnt := make(map[int][]int)
		DPrintf("server:%v idx:%v", sc.me, commandIndex)
		DPrintf("leave groups size:%v", len(op.GIDs))
		DPrintf("old config group size:%v", len(oldConfig.Groups))
		for g, _ := range oldConfig.Groups {
			DPrintf("gid:%v", g)
		}
		for idx, GID := range oldConfig.Shards {
			exists := false
			for _, id := range op.GIDs {
				DPrintf("leave:%v origin:%v", id, GID)
				if id == GID {
					exists = true
					break
				}
			}
			if exists {
				remainingShards = append(remainingShards, idx)
			} else {
				shardCnt[GID] = append(shardCnt[GID], idx)
			}
		}
		for GID, _ := range oldConfig.Groups {
			exists := false
			for _, id := range op.GIDs {
				if id == GID {
					exists = true
					break
				}
			}
			if _, ok := shardCnt[GID]; !ok && !exists {
				// means this group doesn't own any shard
				shardCnt[GID] = []int{}
			}
		}

		if len(newConfig.Groups) == 0 {
			for i := 0; i < NShards; i++ {
				newConfig.Shards[i] = 0
			}
			sc.configs = append(sc.configs, newConfig)
			sc.lastApplied = commandIndex
			sc.sendReply(commandIndex, &reply)
			return
		}

		shard := NShards / (len(newConfig.Groups))
		remaining := NShards % (len(newConfig.Groups))
		DPrintf("shard:%v", shard)
		DPrintf("remaining size:%v, shardCnt size:%v",
			len(remainingShards), len(shardCnt))

		for s, g := range shardCnt {
			DPrintf("g:%v shard size:%v", s, len(g))
		}

		cnt := 0
		shardIdx := 0
		var keys []int
		for k := range shardCnt {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, GID := range keys {
			shards := shardCnt[GID]
			if len(shards) == shard+1 {
				cnt++
				continue
			}
			bound := shardIdx + shard - len(shards)
			DPrintf("len(shards):%v", len(shards))
			if cnt < remaining {
				bound++
				cnt++
			}
			if shardIdx >= len(remainingShards) {
				break
			}
			if bound > len(remainingShards) {
				break
			}
			DPrintf("shardIdx:%v, bound:%v", shardIdx, bound)
			shardCnt[GID] = append(shardCnt[GID],
				remainingShards[shardIdx:bound]...)
			shardIdx = bound
		}

		//for s, g := range shardCnt {
		//	DPrintf("g:%v shard size:%v", s, len(g))
		//}

		for group, shards := range shardCnt {
			for _, s := range shards {
				newConfig.Shards[s] = group
			}
		}
		for s, g := range newConfig.Shards {
			DPrintf("g:%v, s:%v", s, g)
		}

		sc.configs = append(sc.configs, newConfig)

	}
	sc.lastApplied = commandIndex
	sc.sendReply(commandIndex, &reply)
}

func (sc *ShardCtrler) handleMove(op Op, commandIndex int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply := OpReply{
		Type:        MOVE,
		WrongLeader: false,
	}

	if commandIndex <= sc.lastApplied {
		DPrintf("outdated command in %v", sc.me)
		return
	}

	if _, exists := sc.ids[op.Uuid]; exists && sc.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)

	} else {
		sc.ids[op.Uuid] = op.Count

		oldConfig := sc.configs[len(sc.configs)-1]
		newConfig := Config{
			Num:    oldConfig.Num + 1,
			Groups: make(map[int][]string),
			//Shards: [NShards]int,
		}
		// copy the old groups
		for k, v := range oldConfig.Groups {
			newConfig.Groups[k] = v
		}
		for shard, GID := range oldConfig.Shards {
			if shard == op.Shard {
				newConfig.Shards[shard] = op.GID
			} else {
				newConfig.Shards[shard] = GID
			}
		}

		sc.configs = append(sc.configs, newConfig)
	}

	sc.lastApplied = commandIndex
	sc.sendReply(commandIndex, &reply)
}

func (sc *ShardCtrler) handleQuery(op Op, commandIndex int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	reply := OpReply{
		Type:        QUERY,
		WrongLeader: false,
	}

	if commandIndex <= sc.lastApplied {
		DPrintf("outdated command in %v", sc.me)
		return
	}

	if op.Num < 0 || op.Num >= len(sc.configs) {
		reply.Conf = sc.configs[len(sc.configs)-1]
		DPrintf("%v got an invalid num query, newest config num %v",
			sc.me, sc.configs[len(sc.configs)-1].Num)
	} else {
		reply.Conf = sc.configs[op.Num]
	}

	sc.lastApplied = commandIndex
	sc.sendReply(commandIndex, &reply)
}

func (sc *ShardCtrler) checkCh() {
	for !sc.killed() {
		commandMsg := <-sc.applyCh
		if commandMsg.CommandValid {
			// get a request from client(through raft)
			if op, ok := commandMsg.Command.(Op); ok {
				switch op.Type {
				case JOIN:
					sc.handleJoin(op, commandMsg.CommandIndex)
				case LEAVE:
					sc.handleLeave(op, commandMsg.CommandIndex)
				case MOVE:
					sc.handleMove(op, commandMsg.CommandIndex)
				case QUERY:
					sc.handleQuery(op, commandMsg.CommandIndex)
				}
			}
		} else {
			// get a snapshot request
			snapshot := commandMsg.Snapshot
			sc.mu.Lock()
			DPrintf("%v gets a snapshot msg", sc.me)
			sc.readSnapshot(snapshot)
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) readSnapshot(snapshot []byte) int {

	if snapshot == nil || len(snapshot) < 1 {
		return 1
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var configs []Config
	var ids map[int64]int
	//var records map[int]OpMsg
	var lastApplied int
	if err := d.Decode(&configs); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&ids); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	//if err := d.Decode(&records); err != nil {
	//	DPrintf("Decode error!")
	//	return 2
	//}
	if err := d.Decode(&lastApplied); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if sc.lastApplied >= lastApplied {
		// outdated snapshot, we can just ignore
		DPrintf("outdated snapshot in %v, lastApplied:%v", sc.me, lastApplied)
		return 1
	}
	DPrintf("%v read snapshot, lastApplied:%v", sc.me, lastApplied)
	sc.configs = configs
	sc.ids = ids
	//kv.records = records
	sc.lastApplied = lastApplied
	return 0
}

func (sc *ShardCtrler) registerReplyCh(index int) chan OpReply {
	replyCh, ok := sc.replyChs[index]
	if !ok {
		sc.replyChs[index] = make(chan OpReply)
		return sc.replyChs[index]
	}

	// if this index's slot has been occupied,
	// then the server may become follower,
	// so we should notify the client that error wrong leader
	select {
	case replyCh <- OpReply{
		WrongLeader: true,
	}:
		return replyCh
	case <-time.After(sendTimeout * time.Millisecond):
		return replyCh
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:    JOIN,
		Uuid:    args.Uuid,
		Count:   args.Count,
		Servers: args.Servers,
	}
	reply.WrongLeader = true
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	sc.mu.Lock()
	replyCh := sc.registerReplyCh(index)
	sc.mu.Unlock()

	op.Term = term
	DPrintf("%v get a join request %v", sc.me, index)

	select {
	case opReply := <-replyCh:
		reply.WrongLeader = opReply.WrongLeader

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", sc.me)
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:  LEAVE,
		Uuid:  args.Uuid,
		Count: args.Count,
		GIDs:  args.GIDs,
	}
	reply.WrongLeader = true
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	sc.mu.Lock()
	replyCh := sc.registerReplyCh(index)
	sc.mu.Unlock()

	op.Term = term
	DPrintf("%v get a leave request %v", sc.me, index)

	select {
	case opReply := <-replyCh:
		reply.WrongLeader = opReply.WrongLeader

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", sc.me)
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:  MOVE,
		Uuid:  args.Uuid,
		Count: args.Count,
		Shard: args.Shard,
		GID:   args.GID,
	}
	reply.WrongLeader = true
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	sc.mu.Lock()
	replyCh := sc.registerReplyCh(index)
	sc.mu.Unlock()

	op.Term = term
	DPrintf("%v get a move request %v", sc.me, index)

	select {
	case opReply := <-replyCh:
		reply.WrongLeader = opReply.WrongLeader

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", sc.me)
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:  QUERY,
		Uuid:  args.Uuid,
		Count: args.Count,
		Num:   args.Num,
	}
	reply.WrongLeader = true
	index, term, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	sc.mu.Lock()
	replyCh := sc.registerReplyCh(index)
	sc.mu.Unlock()

	op.Term = term
	DPrintf("%v get a query request %v", sc.me, index)

	select {
	case opReply := <-replyCh:
		reply.WrongLeader = opReply.WrongLeader
		reply.Config = opReply.Conf

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", sc.me)
		return
	}
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//// checkStateSize
//// check whether the size of the server is close to maxraftstate
//func (sc *ShardCtrler) checkStateSize() {
//	for {
//		if sc.maxraftstate == -1 {
//			return
//		}
//		kv.mu.Lock()
//		if kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate+100 {
//			w := new(bytes.Buffer)
//			e := labgob.NewEncoder(w)
//			// 保存键值对和所有的uuid
//			err := e.Encode(kv.kvs)
//			if err != nil {
//				DPrintf("encode error")
//				kv.mu.Unlock()
//				return
//			}
//			err = e.Encode(kv.ids)
//			if err != nil {
//				DPrintf("encode error")
//				kv.mu.Unlock()
//				return
//			}
//			//e.Encode(kv.records)
//			err = e.Encode(kv.lastApplied)
//			if err != nil {
//				DPrintf("encode error")
//				kv.mu.Unlock()
//				return
//			}
//			data := w.Bytes()
//			kv.rf.Snapshot(kv.lastApplied, data)
//			DPrintf("%v calls snapshot, lastApplied:%v", kv.me, kv.lastApplied)
//		}
//		kv.mu.Unlock()
//		time.Sleep(5 * time.Millisecond)
//	}
//}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.mu.Lock()
	if sc.readSnapshot(sc.rf.GetPersister().ReadSnapshot()) != 0 {
		DPrintf("no snapshot in %v", sc.me)
		sc.ids = make(map[int64]int)
		sc.lastApplied = 0
	}
	sc.mu.Unlock()

	sc.replyChs = make(map[int]chan OpReply)

	go sc.checkCh()

	return sc
}
