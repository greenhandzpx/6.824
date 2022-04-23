package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	Uuid  int64 // 每个请求的唯一id
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
	// 存每一个请求的id，防止重复
	ids map[int64]void
}

func (kv *KVServer) checkCh() {
	for kv.killed() == false {
		//DPrintf("%v check the channel", kv.me)
		commandMsg := <-kv.applyCh
		//DPrintf("%v got one apply msg", kv.me)
		if commandMsg.CommandValid {
			// 如果是请求
			DPrintf("%v got a request(index:%v)", kv.me, commandMsg.CommandIndex)
			if op, ok := commandMsg.Command.(Op); ok {
				if op.Type == GET {
					// GET请求
					reply := OpReply{
						Type: GET,
					}
					kv.mu.Lock()
					if _, exists := kv.ids[op.Uuid]; !exists {
						kv.ids[op.Uuid] = void{}
					}
					value, ok := kv.kvs[op.Key]
					if ok {
						reply.Err = OK
						reply.Value = value
					} else {
						DPrintf("No value for key %v", op.Key)
						reply.Err = ErrNoKey
						reply.Value = ""
					}
					kv.records[commandMsg.CommandIndex] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.mu.Unlock()

				} else if op.Type == PUT {
					// PUT请求
					reply := OpReply{
						Type: PUT,
						Err:  OK,
					}
					kv.mu.Lock()
					if _, exists := kv.ids[op.Uuid]; exists {
						DPrintf("uuid %v already exists", op.Uuid)
					} else {
						kv.ids[op.Uuid] = void{}
						DPrintf("%v add key:%v value:%v", kv.me, op.Key, op.Value)
						kv.kvs[op.Key] = op.Value
					}
					kv.records[commandMsg.CommandIndex] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.mu.Unlock()
				} else {
					// APPEND请求
					//appendMsg, _ := op.Args.(PutAppendArgs)
					reply := OpReply{
						Type: APPEND,
					}
					kv.mu.Lock()
					if _, exists := kv.ids[op.Uuid]; exists {
						DPrintf("uuid %v already exists", op.Uuid)
						reply.Err = OK
					} else {
						kv.ids[op.Uuid] = void{}
						_, ok := kv.kvs[op.Key]
						DPrintf("%v add key:%v value:%v", kv.me, op.Key, op.Value)
						if ok {
							reply.Err = OK
							kv.kvs[op.Key] += op.Value
						} else {
							reply.Err = ErrNoKey
							kv.kvs[op.Key] = op.Value
						}
					}
					kv.records[commandMsg.CommandIndex] = OpMsg{
						Request:  op,
						Response: reply,
						Done:     true,
					}
					kv.mu.Unlock()
				}
			}

		}
		//time.Sleep(30 * time.Millisecond)
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
	kv.records[index] = opMsg
	kv.mu.Unlock()

	startTime := time.Now().UnixMilli()
	for {
		if time.Now().UnixMilli()-startTime > 1000 {
			// 大于3秒则返回
			DPrintf("time out ")
			return
		}
		kv.mu.Lock()
		if kv.records[index].Done {
			if kv.records[index].Request != op {
				// 说明不是原来的请求
				kv.mu.Unlock()
				return
			}
			reply.Err = kv.records[index].Response.Err
			reply.Value = kv.records[index].Response.Value
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
		Uuid:  args.Token,
	}
	if args.Op == "Put" {
		op.Type = PUT
	} else {
		op.Type = APPEND
	}
	reply.Err = ErrWrongLeader
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	reply.LeaderId = kv.me

	kv.mu.Lock()
	kv.records[index] = OpMsg{
		Request: op,
		Done:    false,
	}
	kv.mu.Unlock()
	DPrintf("%v got a put or append request", kv.me)
	startTime := time.Now().UnixMilli()
	for {
		if time.Now().UnixMilli()-startTime > 1000 {
			DPrintf("time out ")
			return
		}
		kv.mu.Lock()
		if kv.records[index].Done {
			if kv.records[index].Request != op {
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

	// You may need initialization code here.
	kv.records = make(map[int]OpMsg)
	kv.kvs = make(map[string]string)
	kv.ids = make(map[int64]void)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.checkCh()
	return kv
}
