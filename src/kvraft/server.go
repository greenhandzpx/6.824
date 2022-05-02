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
}
type OpReply struct {
	Type  int
	Value string
	Err   Err
}

type OpMsg struct {
	Request  Op
	Response OpReply
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
	// 相当于log，记录每一个客户端发来的entry
	records map[int]OpMsg
	// 存每一个客户的最新请求的id，防止重复
	ids map[int64]int
	// 最后一个已经执行的请求的index
	// 注：kv.lastApplied <= kv.rf.lastApplied
	lastApplied int
}

func (kv *KVServer) checkCh() {
	for kv.killed() == false {
		//DPrintf("%v check the channel", kv.me)
		commandMsg := <-kv.applyCh
		//DPrintf("%v got one apply msg", kv.me)
		if commandMsg.CommandValid {
			// 如果是操作键值对的请求
			DPrintf("%v got a request(index:%v)", kv.me, commandMsg.CommandIndex)
			if op, ok := commandMsg.Command.(Op); ok {
				if op.Type == GET {
					// GET请求
					reply := OpReply{
						Type: GET,
					}
					kv.mu.Lock()
					if commandMsg.CommandIndex <= kv.lastApplied {
						DPrintf("outdated command in %v", kv.me)
						kv.mu.Unlock()
						continue
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
					kv.records[commandMsg.CommandIndex-1] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.lastApplied = commandMsg.CommandIndex
					kv.mu.Unlock()

				} else if op.Type == PUT {
					// PUT请求
					reply := OpReply{
						Type: PUT,
						Err:  OK,
					}
					kv.mu.Lock()
					if commandMsg.CommandIndex <= kv.lastApplied {
						DPrintf("outdated command in %v", kv.me)
						kv.mu.Unlock()
						continue
					}
					if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
						DPrintf("count %v already exists", op.Count)
					} else {
						kv.ids[op.Uuid] = op.Count
						DPrintf("%v add key:%v value:%v", kv.me, op.Key, op.Value)
						kv.ids[op.Uuid] = op.Count
						kv.kvs[op.Key] = op.Value
					}
					kv.records[commandMsg.CommandIndex-1] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.lastApplied = commandMsg.CommandIndex
					kv.mu.Unlock()
				} else {
					// APPEND请求
					//appendMsg, _ := op.Args.(PutAppendArgs)
					reply := OpReply{
						Type: APPEND,
					}
					kv.mu.Lock()
					if commandMsg.CommandIndex <= kv.lastApplied {
						DPrintf("outdated command in %v", kv.me)
						kv.mu.Unlock()
						continue
					}
					if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
						DPrintf("count %v already exists", op.Count)
						reply.Err = OK
					} else {
						kv.ids[op.Uuid] = op.Count
						DPrintf("%v add key:%v value:%v", kv.me, op.Key, op.Value)
						_, ok := kv.kvs[op.Key]
						if ok {
							reply.Err = OK
							kv.kvs[op.Key] += op.Value
						} else {
							reply.Err = ErrNoKey
							kv.kvs[op.Key] = op.Value
						}
					}
					//if _, exists := kv.ids[op.Uuid]; exists {
					//	DPrintf("uuid %v already exists", op.Uuid)
					//} else {
					//	kv.ids[op.Uuid] = void{}
					//}
					kv.records[commandMsg.CommandIndex-1] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.lastApplied = commandMsg.CommandIndex
					kv.mu.Unlock()
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Key:  args.Key,
		Type: GET,
		Uuid: args.Token,
	}
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	reply.LeaderId = kv.me
	kv.mu.Lock()
	opMsg := OpMsg{
		Request: op,
		Done:    false,
	}
	kv.records[index-1] = opMsg
	kv.mu.Unlock()

	startTime := time.Now().UnixMilli()
	for {
		if time.Now().UnixMilli()-startTime > 350 {
			// 大于500ms则返回
			DPrintf("time out ")
			return
		}
		kv.mu.Lock()
		if kv.records[index-1].Done {
			if kv.records[index-1].Request != op {
				// 说明不是原来的请求
				kv.mu.Unlock()
				return
			}
			reply.Err = kv.records[index-1].Response.Err
			reply.Value = kv.records[index-1].Response.Value
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	rpc_start := time.Now().UnixMilli()
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Uuid:  args.Uuid,
		Count: args.Count,
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
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	reply.LeaderId = kv.me

	kv.mu.Lock()
	kv.records[index-1] = OpMsg{
		Request: op,
		Done:    false,
	}
	kv.mu.Unlock()
	DPrintf("%v got a put or append request", kv.me)
	startTime := time.Now().UnixMilli()
	for {
		if time.Now().UnixMilli()-startTime > 350 {
			DPrintf("time out ")
			return
		}
		kv.mu.Lock()
		if kv.records[index-1].Done {
			if kv.records[index-1].Request != op {
				DPrintf("not the same request at the same index %v", index)
				kv.mu.Unlock()
				return
			}
			reply.Err = kv.records[index].Response.Err
			DPrintf("rpc time pass: %v", time.Now().UnixMilli()-rpc_start)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
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
				kv.mu.Unlock()
				return
			}
			err = e.Encode(kv.ids)
			if err != nil {
				kv.mu.Unlock()
				return
			}
			// records 也许不用存？
			//e.Encode(kv.records)
			err = e.Encode(kv.lastApplied)
			if err != nil {
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
		kv.kvs = make(map[string]string)
		kv.ids = make(map[int64]int)
		kv.lastApplied = 0
	}
	kv.mu.Unlock()
	kv.records = make(map[int]OpMsg)

	go kv.checkCh()
	go kv.checkStateSize()
	return kv
}
