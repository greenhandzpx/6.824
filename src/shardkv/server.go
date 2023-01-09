package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = true

const (
	GET           = 0
	PUT           = 1
	APPEND        = 2
	MIGRATE       = 3 // config has changed
	GETSHARDS     = 4 // the new group fetch shards from old group
	HANDOUTSHARDS = 5 // the old group give shards to new group
)

const (
	recvTimeout = 500
	sendTimeout = 100
	migrateTimeout = 400
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	// GET
	Key string

	// PUT/APPEND
	Value string

	// MIGRATE & HANDOUTSHARDS & GETSHARDS
	Conf shardctrler.Config

	// GETSHARDS
	Shard int
	// HANDOUTSHARDS
	HandoutErr Err
	Kvs map[string]string


	Uuid  int64 // 每个客户的唯一id
	Count int   // the count of the requests of this client
	//ReplyCh chan OpReply // 接收server的回复
	Term int
}

type OpReply struct {
	Type  int
	Value string
	Err   Err
}
type Dummy struct{}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	uuid  int64
	count int

	configOutstanding int // the latest cofig that is being handled
	// true if the server is fetching shards from other groups
	migrating bool

	config shardctrler.Config // the server's config now
	
	// the config that's being handled(waiting for other groups' kv)
	pendingConfig shardctrler.Config 
	// check whether we have collected all shards according to the pending config
	pendingShards map[string]Dummy
	// last timestamp that the checkConfig function send log to raft	
	lastMigrateTime int64
	// indicate every shard's config num
	shardVersions []int

	//// all the shards that it controls
	//shards map[int]Dummy
	//configNum int

	mck *shardctrler.Clerk

	dead int32
	// 键值集合
	kvs map[string]string

	replyChs map[int]chan OpReply

	// 存每一个客户的最新请求的id，防止重复
	ids map[int64]int
	// 最后一个已经执行的请求的index
	// 注：kv.lastApplied <= kv.rf.lastApplied
	lastApplied int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	replyCh := make(chan OpReply)
	op := Op{
		Key:  args.Key,
		Type: GET,
		//ReplyCh: replyCh,
	}
	reply.Err = ErrWrongLeader
	//index, _, isLeader := kv.rf.Start(op)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	kv.mu.Lock()
	// if kv.migrating {
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }

	replyCh = kv.registerReplyCh(index)
	kv.mu.Unlock()

	op.Term = term
	DPrintf("%v(%v) get a get request %v(k:%v)", kv.me, kv.gid, index, args.Key)
	reply.LeaderId = kv.me

	// TODO
	// change polling to channel or condition valuable
	select {
	case opReply := <-replyCh:
		reply.Err = opReply.Err
		reply.Value = opReply.Value
		DPrintf("%v(%v) get a get reply %v(k:%v v:%v)",
			kv.me, kv.gid, index, args.Key, reply.Value)
		return

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v(%v) time out, idx %v", kv.me, kv.gid, index)
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//rpc_start := time.Now().UnixMilli()
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
	// if kv.migrating {
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }

	replyCh := kv.registerReplyCh(index)
	kv.mu.Unlock()

	op.Term = term
	DPrintf("%v(%v) got a put or append request %v(k:%v, v:%v)",
		kv.me, kv.gid, index, args.Key, args.Value)
	reply.LeaderId = kv.me

	// TODO
	// change polling to channel or condition valuable
	select {
	case opReply := <-replyCh:
		reply.Err = opReply.Err
		DPrintf("%v(%v) got a put or append reply %v", kv.me, kv.gid, index)
		return

	case <-time.After(recvTimeout * time.Millisecond):
		DPrintf("%v(%v) time out, idx %v", kv.me, kv.gid, index)
		return
	}

}

func (kv *ShardKV) GetShards(args *GetShardsArgs, reply *GetShardsReply) {
	DPrintf("%v(%v) getshards rpc", kv.me, kv.gid)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// TODO not sure dead lock
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("%v(%v) got a get_shards(shard %v) request from group %v", kv.me, kv.gid, args.Shard, args.Gid)

	op := Op{
		Type: GETSHARDS,
		Conf: args.Config,
	}
	
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK

	// // DPrintf("%v(%v) grants some kvs", kv.me, kv.gid)
	// replyKvs := make(map[string]string)

	// for k, v := range kv.kvs {
	// 	if key2shard(k) == args.Shard {
	// 		replyKvs[k] = v
	// 	}
	// }
	// reply.Kvs = replyKvs
	// reply.Err = OK
}

func (kv *ShardKV) HandoutShards(args *HandoutShardsArgs, reply *HandoutShardsReply) {
	DPrintf("%v(%v) handoutshards rpc", kv.me, kv.gid)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return 
	}

	DPrintf("%v(%v) got a handout_shards(shard %v) request from group %v", kv.me, kv.gid, args.Shard, args.Gid)

	op := Op {
		Type: HANDOUTSHARDS,
		Conf: args.Config,
		Kvs: args.Kvs,
		Shard: args.Shard,
		HandoutErr: args.Err,
	}

	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
}

func (kv *ShardKV) readSnapshot(snapshot []byte) int {
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
		DPrintf("outdated snapshot in %v(%v), lastApplied:%v", kv.me, kv.gid, lastApplied)
		return 1
	}
	DPrintf("%v(%v) read snapshot, lastApplied:%v", kv.me, kv.gid, lastApplied)
	kv.kvs = kvs
	kv.ids = ids
	//kv.records = records
	kv.lastApplied = lastApplied
	return 0
}


// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartServer
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
//
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
	kv.dead = 0

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.uuid = nrand()
	kv.count = 0
	kv.configOutstanding = 0
	kv.migrating = false

	kv.pendingShards = make(map[string]Dummy)
	kv.lastMigrateTime = 0

	kv.shardVersions = make([]int, shardctrler.NShards)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shardVersions[i] = 0
	}
	//kv.shards = make(map[int]Dummy)
	//
	//kv.configNum = 0

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
	go kv.checkConfig()

	return kv
}
