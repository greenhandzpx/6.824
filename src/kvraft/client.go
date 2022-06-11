package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	lastLeader int   // 上一个请求的leader
	uuid       int64 // 每个clerk的唯一标识
	count      int   // 对请求进行计数
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.uuid = nrand()
	ck.count = 0
	return ck
}

//
func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
	return ok
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{
		Key: key,
	}
	DPrintf("client %v send a get request", ck.uuid)
	DPrintf("leader %v", ck.lastLeader)
	ck.mu.Unlock()
	for k := ck.lastLeader; ; k++ {
		i := k % len(ck.servers)
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrWrongLeader {
			ck.mu.Lock()
			DPrintf("wrong leader %v", i)
			ck.mu.Unlock()
			continue
		}
		ck.mu.Lock()
		ck.lastLeader = i
		//ck.lastLeader = reply.LeaderId
		ck.mu.Unlock()
		return reply.Value
	}

}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Uuid:  ck.uuid,
	}
	ck.mu.Lock()
	args.Count = ck.count
	ck.count++
	DPrintf("client %v send a put or append request(k:%v v:%v)",
		ck.uuid, key, value)
	DPrintf("leader %v", ck.lastLeader)
	//startTime := time.Now().UnixMilli()
	ck.mu.Unlock()
	for k := ck.lastLeader; ; k++ {
		i := k % len(ck.servers)
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("rpc fail")
			continue
		}
		if reply.Err == ErrWrongLeader {
			DPrintf("wrong leader %v", i)
			//DPrintf("wrong leader %v", ck.lastLeader)
			continue
		}
		ck.mu.Lock()
		ck.lastLeader = i
		//DPrintf("last leader is %v, time pass: %v", ck.lastLeader, time.Now().UnixMilli()-startTime)
		ck.mu.Unlock()
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
