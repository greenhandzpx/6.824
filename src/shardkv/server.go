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
	HANDOUTSHARDS = 4 // leader fetch shards and want to hand out to followers
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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type int
	// GET
	Key string

	// PUT/APPEND
	Value string

	// MIGRATE & HANDOUTSHARDS
	Conf shardctrler.Config

	// HANDOUTSHARDS
	Kvs map[string]string
	//ConfigNum int

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

	config shardctrler.Config
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

func (kv *ShardKV) sendReply(commandIndex int, reply *OpReply) {
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
		DPrintf("%v(%v) isn't leader, shouldn't send reply", kv.me, kv.gid)
		return
	}
	go func() {
		select {
		//case op.ReplyCh <- reply:
		case replyCh <- *reply:
			if reply.Type == GET {
				DPrintf("%v(%v) send a get reply, %v", kv.me, kv.gid, commandIndex)
			} else if reply.Type == PUT ||
				reply.Type == APPEND {
				DPrintf("%v(%v) send a put/append reply, %v", kv.me, kv.gid, commandIndex)
			}
			//kv.mu.Lock()
			//kv.lastApplied = commandIndex
			//kv.mu.Unlock()
		case <-time.After(sendTimeout * time.Millisecond):
			DPrintf("%v(%v) send a reply %v timeout", kv.me, kv.gid, commandIndex)
			return
		}
	}()
}

func (kv *ShardKV) handleGetReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: GET,
	}
	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	//if kv.migrating {
	//	// not
	//}

	// TODO: should stop all the requests
	if kv.migrating {
		// the server is migrating, should not accept any requests
		reply.Err = ErrWrongGroup
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		DPrintf("%v(%v) error wrong group, idx:%v", kv.me, kv.gid, commandIndex)
		kv.sendReply(commandIndex, &reply)
		return
	}

	//if _, ok := kv.shards[key2shard(op.Key)]; !ok {
	//	// The config changed, and this shard isn't controlled by this server
	//	reply.Err = ErrWrongGroup
	//	return
	//}
	//if _, exists := kv.ids[op.Uuid]; !exists {
	//	kv.ids[op.Uuid] = void{}
	//}
	value, ok := kv.kvs[op.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
		DPrintf("(idx:%v)%v(%v) get key:%v value:%v", commandIndex, kv.me, kv.gid, op.Key, value)
	} else {
		DPrintf("No value for key %v", op.Key)
		reply.Err = ErrNoKey
		reply.Value = ""
	}

	kv.lastApplied = commandIndex
	//kv.lastApplied++

	kv.sendReply(commandIndex, &reply)
}

func (kv *ShardKV) handleAppendReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: APPEND,
	}

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	if kv.migrating {
		// the server is migrating, should not accept any requests
		reply.Err = ErrWrongGroup
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		DPrintf("%v(%v) error wrong group, idx:%v", kv.me, kv.gid, commandIndex)
		kv.sendReply(commandIndex, &reply)
		return
	}
	//if _, ok := kv.shards[key2shard(op.Key)]; !ok {
	//	// The config changed, and this shard isn't controlled by this server
	//	reply.Err = ErrWrongGroup
	//	return
	//}

	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
		reply.Err = OK
	} else {
		kv.ids[op.Uuid] = op.Count
		_, ok := kv.kvs[op.Key]
		if ok {
			reply.Err = OK
			kv.kvs[op.Key] += op.Value
			DPrintf("(idx:%v)%v(%v) append key:%v value:%v", commandIndex, kv.me, kv.gid, op.Key, op.Value)
			DPrintf("%v(%v) now k:%v, v:%v", kv.me, kv.gid, op.Key, kv.kvs[op.Key])
		} else {
			reply.Err = ErrNoKey
			kv.kvs[op.Key] = op.Value
			DPrintf("%v(%v) put(append) key:%v value:%v", kv.me, kv.gid, op.Key, op.Value)
		}
	}

	kv.lastApplied = commandIndex
	//kv.lastApplied++
	kv.sendReply(commandIndex, &reply)
}

func (kv *ShardKV) handlePutReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := OpReply{
		Type: PUT,
		Err:  OK,
	}

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	if kv.migrating {
		// the server is migrating, should not accept any requests
		reply.Err = ErrWrongGroup
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		DPrintf("%v(%v) error wrong group, idx:%v", kv.me, kv.gid, commandIndex)
		kv.sendReply(commandIndex, &reply)
		return
	}
	//if _, ok := kv.shards[key2shard(op.Key)]; !ok {
	//	// The config changed, and this shard isn't controlled by this server
	//	reply.Err = ErrWrongGroup
	//	return
	//}

	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
	} else {
		kv.ids[op.Uuid] = op.Count
		DPrintf("(idx:%v)%v(%v) put key:%v value:%v", commandIndex, kv.me, kv.gid, op.Key, op.Value)
		kv.ids[op.Uuid] = op.Count
		kv.kvs[op.Key] = op.Value
	}
	kv.lastApplied = commandIndex
	//kv.lastApplied++
	kv.sendReply(commandIndex, &reply)

}

func (kv *ShardKV) handleMigrateReq(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	if op.Conf.Num <= kv.config.Num || op.Conf.Num < kv.configOutstanding {
		// means this config is outdated
		DPrintf("outdated config update %v in %v(%v)", op.Conf.Num, kv.me, kv.gid)
		return
	}

	//if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
	//	DPrintf("count %v already exists", op.Count)
	//	return
	//}

	//kv.ids[op.Uuid] = op.Count

	// set this state to be true,
	// in order that before finishing migrating, we won't accept any requests
	kv.migrating = true
	kv.configOutstanding = op.Conf.Num

	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("%v(%v) isn't leader, shouldn't handle migrate", kv.me, kv.gid)
		kv.lastApplied = commandIndex
		return
	}

	DPrintf("%v(%v) get a migrate request %v", kv.me, kv.gid, commandIndex)

	handoutKvs := make(map[string]string)

	if kv.config.Num == 0 {
		// means this server just started
		kv.lastApplied = commandIndex
		newOp := Op{
			Type: HANDOUTSHARDS,
			Conf: op.Conf,
			Kvs:  handoutKvs,
		}
		kv.rf.Start(newOp)
		return
	}

	// update the shards set
	for shard, gid := range op.Conf.Shards {
		if kv.gid != gid {
			continue
		}
		if kv.config.Shards[shard] == gid {
			continue
		}

		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}

		DPrintf("%v(%v) should have a new shard %v, gid %v",
			kv.me, kv.gid, shard, gid)

		args := GetShardsArgs{
			Shard: shard,
		}
		targetGid := kv.config.Shards[shard]
		// only the leader sends rpc request to other groups
		// to fetch shards
		kv.mu.Unlock()
		for {
			success := false
			DPrintf("%v(%v) fetch shards from group, size:%v",
				kv.me, kv.gid, len(kv.config.Groups[targetGid]))
			for _, srv := range kv.config.Groups[targetGid] {
				server := kv.make_end(srv)
				var replyTmp GetShardsReply

				DPrintf("fetch shard before")
				ok := server.Call("ShardKV.GetShards", &args, &replyTmp)
				DPrintf("fetch shard after")

				if ok && replyTmp.Err == OK {
					success = true
					for k, v := range replyTmp.Kvs {
						handoutKvs[k] = v
					}
					break
				}
			}
			if success {
				break
			}
		}
		kv.mu.Lock()
	}
	// When we fetch all required shards, we send them to the raft
	// and let all the other servers get those shards.
	DPrintf("%v(%v) finish fetching, start handout", kv.me, kv.gid)
	kv.lastApplied = commandIndex
	newOp := Op{
		Type: HANDOUTSHARDS,
		Conf: op.Conf,
		Kvs:  handoutKvs,
	}
	kv.rf.Start(newOp)

	// only when we get the raft shards, will we update the config
	//kv.config = op.Conf
}

func (kv *ShardKV) handleHandoutShards(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	if op.Conf.Num <= kv.config.Num || op.Conf.Num < kv.configOutstanding {
		// outdated shards
		return
	}

	// add these kvs to the kv database
	for k, v := range op.Kvs {
		kv.kvs[k] = v
	}
	kv.config = op.Conf
	kv.migrating = false
	kv.configOutstanding = 0

}

func (kv *ShardKV) checkCh() {
	for kv.killed() == false {
		//DPrintf("%v check the channel", kv.me)
		commandMsg := <-kv.applyCh
		//DPrintf("%v got one apply msg", kv.me)
		if commandMsg.CommandValid {
			// 如果是操作键值对的请求
			//DPrintf("%v got a request(index:%v)", kv.me, commandMsg.CommandIndex)
			if op, ok := commandMsg.Command.(Op); ok {
				if op.Type == GET {
					// GET request
					kv.handleGetReq(op, commandMsg.CommandIndex)
				} else if op.Type == PUT {
					// PUT request
					kv.handlePutReq(op, commandMsg.CommandIndex)
				} else if op.Type == APPEND {
					// APPEND request
					kv.handleAppendReq(op, commandMsg.CommandIndex)
				} else if op.Type == MIGRATE {
					// Migrate request
					kv.handleMigrateReq(op, commandMsg.CommandIndex)
				} else if op.Type == HANDOUTSHARDS {
					// Handout Shards request
					kv.handleHandoutShards(op, commandMsg.CommandIndex)
				}
			}

		} else {
			// 如果是快照的消息
			snapshot := commandMsg.Snapshot
			kv.mu.Lock()
			DPrintf("%v(%v) gets a snapshot msg", kv.me, kv.gid)
			kv.readSnapshot(snapshot)
			kv.mu.Unlock()
		}
	}

}

// When a new request comes, it registers a channel in the server.
// When the request is committed by raft, server handles the request and
// sends the result to the client using this channel.
func (kv *ShardKV) registerReplyCh(index int) chan OpReply {
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
	if kv.migrating {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

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
	if kv.migrating {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

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

	DPrintf("%v(%v) grants some kvs", kv.me, kv.gid)
	replyKvs := make(map[string]string)

	for k, v := range kv.kvs {
		if key2shard(k) == args.Shard {
			replyKvs[k] = v
		}
	}
	reply.Kvs = replyKvs
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

// checkStateSize
// 检查server的字节数是否接近maxraftstate
func (kv *ShardKV) checkStateSize() {
	for kv.killed() == false {
		if kv.maxraftstate == -1 {
			return
		}
		kv.mu.Lock()
		if kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate+100 {
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
			//e.Encode(kv.records)
			err = e.Encode(kv.lastApplied)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			data := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, data)
			DPrintf("%v(%v) calls snapshot, lastApplied:%v", kv.me, kv.gid, kv.lastApplied)
		}
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// checkConfig
// poll the shard controller to get the latest config
func (kv *ShardKV) checkConfig() {

	for kv.killed() == false {

		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}
		newConfig := kv.mck.Query(-1)

		op := Op{}
		kv.mu.Lock()
		if newConfig.Num > kv.config.Num && newConfig.Num >= kv.configOutstanding {
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.mu.Unlock()
				continue
			}
			DPrintf("config has updated, ser:%v(%v), newNum:%v, oldNum:%v",
				kv.me, kv.gid, newConfig.Num, kv.config.Num)
			kv.mu.Unlock()
			op.Type = MIGRATE
			op.Conf = newConfig
			//op.Uuid = kv.uuid
			//op.Count = kv.count
			//kv.count++
			// Maybe we can just ignore the reply?
			// TODO Not sure here
			kv.rf.Start(op)
			//if isLeader {
			//
			//}
		} else {
			kv.mu.Unlock()
		}

		time.Sleep(80 * time.Millisecond)
	}
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
