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
	token      int64 // 每个请求的唯一标识
	mu         sync.Mutex
	lastLeader int   // 上一个请求的leader
	uuid       int64 // 每个clerk的唯一标识
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
	ck.token = ck.uuid
	return ck
}

//
//func (ck *Clerk) sendGet(args *GetArgs, reply *GetReply) {
//	ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//}

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
		Key:   key,
		Token: ck.token,
	}
	DPrintf("client send a get request, uuid: %v", args.Token)
	ck.token++
	ck.mu.Unlock()
	for k := ck.lastLeader; ; k++ {
		i := k % len(ck.servers)
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrWrongLeader {
			continue
		}
		ck.mu.Lock()
		ck.lastLeader = reply.LeaderId
		ck.mu.Unlock()
		return reply.Value
	}

}

//
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
	}
	ck.mu.Lock()
	args.Token = ck.token
	ck.token++
	DPrintf("client send a put or append request, key:%v, value:%v, uuid:%v", key, value, args.Token)
	ck.mu.Unlock()
	for k := ck.lastLeader; ; k++ {
		i := k % len(ck.servers)
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrWrongLeader {
			continue
		}
		ck.mu.Lock()
		ck.lastLeader = reply.LeaderId
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
