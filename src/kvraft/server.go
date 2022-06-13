package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

const (
	GET    = 0
	PUT    = 1
	APPEND = 2
)

const (
	recvTimeout = 500
	sendTimeout = 100
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type  int
	Key   string
	Value string
	Uuid  int64 // 每个客户的唯一id
	Count int
	//ReplyCh chan OpReply // 接收server的回复
	Term int
}

type OpReply struct {
	Type  int
	Value string
	Err   Err
}

type OpMsg struct {
	Request  Op
	Response chan OpReply // every entry keeps a chan for receiving data
	Done     bool
}

type void struct{}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// 键值集合
	kvs map[string]string

	//// 相当于log，记录每一个客户端发来的entry
	//records map[int]OpMsg
	replyChs map[int]chan OpReply

	// 存每一个客户的最新请求的id，防止重复
	ids map[int64]int
	// 最后一个已经执行的请求的index
	// 注：kv.lastApplied <= kv.rf.lastApplied
	lastApplied int
}

func (kv *KVServer) sendReply(commandIndex int, reply *OpReply) {
	replyCh, ok := kv.replyChs[commandIndex]
	if !ok {
		return
	}
	// only leader whose entry's term is the right term should it notify the client
	if _, isLeader := kv.rf.GetState(); !isLeader {
		//select {
		//case replyCh <- OpReply{
		//	Err: ErrWrongLeader,
		//}:
		//	return
		//case <-time.After(sendTimeout * time.Millisecond):
		//	return
		//}
		return
	}
	go func() {
		select {
		//case op.ReplyCh <- reply:
		case replyCh <- *reply:
			DPrintf("%v send a get reply, %v", kv.me, commandIndex)
			kv.mu.Lock()
			kv.lastApplied = commandIndex
			kv.mu.Unlock()
		case <-time.After(sendTimeout * time.Millisecond):
			DPrintf("%v send a get reply %v timeout", kv.me, commandIndex)
			return
		}
	}()

}

func (kv *KVServer) handleGetReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: GET,
	}
	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v", kv.me)
		return
	}
	//if _, exists := kv.ids[op.Uuid]; !exists {
	//	kv.ids[op.Uuid] = void{}
	//}
	value, ok := kv.kvs[op.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		DPrintf("No value for key %v", op.Key)
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	kv.lastApplied++

	kv.sendReply(commandIndex, &reply)

}

func (kv *KVServer) handleAppendReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: APPEND,
	}

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v", kv.me)
		return
	}
	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
		reply.Err = OK
	} else {
		kv.ids[op.Uuid] = op.Count
		_, ok := kv.kvs[op.Key]
		if ok {
			reply.Err = OK
			kv.kvs[op.Key] += op.Value
			DPrintf("%v append key:%v value:%v", kv.me, op.Key, op.Value)
		} else {
			reply.Err = ErrNoKey
			kv.kvs[op.Key] = op.Value
			DPrintf("%v put(append) key:%v value:%v", kv.me, op.Key, op.Value)
		}
	}

	kv.lastApplied++
	kv.sendReply(commandIndex, &reply)
}

func (kv *KVServer) handlePutReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: PUT,
		Err:  OK,
	}

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v", kv.me)
		return
	}

	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
	} else {
		kv.ids[op.Uuid] = op.Count
		DPrintf("%v put key:%v value:%v", kv.me, op.Key, op.Value)
		kv.ids[op.Uuid] = op.Count
		kv.kvs[op.Key] = op.Value
	}
	kv.lastApplied++
	kv.sendReply(commandIndex, &reply)

}

func (kv *KVServer) checkCh() {
	for kv.killed() == false {
		//DPrintf("%v check the channel", kv.me)
		commandMsg := <-kv.applyCh
		//DPrintf("%v got one apply msg", kv.me)
		if commandMsg.CommandValid {
			// 如果是操作键值对的请求
			//DPrintf("%v got a request(index:%v)", kv.me, commandMsg.CommandIndex)
			if op, ok := commandMsg.Command.(Op); ok {
				if op.Type == GET {
					// GET请求
					kv.handleGetReq(op, commandMsg.CommandIndex)
				} else if op.Type == PUT {
					// PUT请求
					kv.handlePutReq(op, commandMsg.CommandIndex)
				} else {
					// APPEND请求
					kv.handleAppendReq(op, commandMsg.CommandIndex)
				}
			}

		} else {
			// 如果是快照的消息
			snapshot := commandMsg.Snapshot
			kv.mu.Lock()
			kv.readSnapshot(snapshot)
			kv.mu.Unlock()
		}
	}

}

// When a new request comes, it registers a channel in the server.
// When the request is committed by raft, server handles the request and
// sends the result to the client using this channel.
func (kv *KVServer) registerReplyCh(index int) chan OpReply {
	replyCh, ok := kv.replyChs[index]
	if !ok {
		kv.replyChs[index] = make(chan OpReply)
		return kv.replyChs[index]
	}

	// if this index's slot has been occupied,
	// then the server may become follower,
	// so we should notify the client that error wrong leader
	select {
	case replyCh <- OpReply{
		Err: ErrWrongLeader,
	}:
		return replyCh
	case <-time.After(sendTimeout * time.Millisecond):
		return replyCh
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	replyCh := make(chan OpReply)
	op := Op{
		Key:  args.Key,
		Type: GET,
		Uuid: args.Token,
		//ReplyCh: replyCh,
	}
	reply.Err = ErrWrongLeader
	//index, _, isLeader := kv.rf.Start(op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	replyCh = kv.registerReplyCh(index)
	kv.mu.Unlock()

	op.Term = term
	DPrintf("%v get a get request %v(k:%v)", kv.me, index, args.Key)
	reply.LeaderId = kv.me

	// TODO
	// change polling to channel or condition valuable
	select {
	case opReply := <-replyCh:
		reply.Err = opReply.Err
		reply.Value = opReply.Value
		DPrintf("%v get a get reply %v(k:%v v:%v)",
			kv.me, index, args.Key, reply.Value)
		return

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", kv.me)
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//rpc_start := time.Now().UnixMilli()
	replyCh := make(chan OpReply)
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Uuid:  args.Uuid,
		Count: args.Count,
		//ReplyCh: replyCh,
	}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	reply.Err = ErrWrongLeader
	kv.mu.Lock()
	if _, exist := kv.ids[args.Uuid]; exist && kv.ids[args.Uuid] >= args.Count {
		// 说明是重复的请求
		reply.Err = OK
		DPrintf("repeat request")
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//index, _, isLeader := kv.rf.Start(op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	replyCh = kv.registerReplyCh(index)
	kv.mu.Unlock()

	op.Term = term
	DPrintf("%v got a put or append request %v(k:%v, v:%v)",
		kv.me, index, args.Key, args.Value)
	reply.LeaderId = kv.me

	// TODO
	// change polling to channel or condition valuable
	select {
	case opReply := <-replyCh:
		reply.Err = opReply.Err
		DPrintf("%v got a put or append reply %v", kv.me, index)
		return

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v time out", kv.me)
		return
	}

}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readSnapshot(snapshot []byte) int {
	//kv.rf.GetPersister().ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return 1
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvs map[string]string
	var ids map[int64]int
	//var records map[int]OpMsg
	var lastApplied int
	if err := d.Decode(&kvs); err != nil {
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
	if kv.lastApplied >= lastApplied {
		// 说明是过期的快照，可以忽略
		DPrintf("outdated snapshot in %v, lastApplied:%v", kv.me, lastApplied)
		return 1
	}
	DPrintf("%v read snapshot, lastApplied:%v", kv.me, lastApplied)
	kv.kvs = kvs
	kv.ids = ids
	//kv.records = records
	kv.lastApplied = lastApplied
	return 0
}

// checkStateSize
// 检查server的字节数是否接近maxraftstate
func (kv *KVServer) checkStateSize() {
	for kv.killed() == false {
		if kv.maxraftstate == -1 {
			return
		}
		kv.mu.Lock()
		if kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate+100 {
			DPrintf("%v calls snapshot, lastApplied:%v", kv.me, kv.lastApplied)
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			// 保存键值对和所有的uuid
			err := e.Encode(kv.kvs)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			err = e.Encode(kv.ids)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			// records 也许不用存？
			//e.Encode(kv.records)
			err = e.Encode(kv.lastApplied)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, data)
		}
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// StartKVServer
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.mu.Lock()
	kv.lastApplied = 0
	if kv.readSnapshot(kv.rf.GetPersister().ReadSnapshot()) != 0 {
		// no snapshot or snapshot error
		DPrintf("no snapshot")
		kv.kvs = make(map[string]string)
		kv.ids = make(map[int64]int)
		kv.lastApplied = 0
	}
	kv.mu.Unlock()
	//kv.records = make(map[int]OpMsg)
	kv.replyChs = make(map[int]chan OpReply)

	go kv.checkCh()
	go kv.checkStateSize()
	return kv
}
