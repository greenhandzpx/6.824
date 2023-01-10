package shardkv

import (
	"bytes"
	"time"

	"6.824/labgob"
	"6.824/shardctrler"
)


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


func (kv *ShardKV) readSnapshot(snapshot []byte) int {
	//kv.rf.GetPersister().ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return 1
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvs map[string]string
	var ids map[int64]int
	var lastApplied int
	var migrating bool
	var config shardctrler.Config
	var pendingConfig shardctrler.Config
	var shardVersions []int

	if err := d.Decode(&kvs); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&ids); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&lastApplied); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if kv.lastApplied >= lastApplied {
		// 说明是过期的快照，可以忽略
		DPrintf("outdated snapshot in %v(%v), lastApplied:%v", kv.me, kv.gid, lastApplied)
		return 1
	}
	if err := d.Decode(&migrating); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&config); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&pendingConfig); err != nil {
		DPrintf("Decode error!")
		return 2
	}
	if err := d.Decode(&shardVersions); err != nil {
		DPrintf("Decode error!")
		return 2
	}

	DPrintf("%v(%v) read snapshot, lastApplied:%v", kv.me, kv.gid, lastApplied)
	kv.kvs = kvs
	kv.ids = ids
	kv.lastApplied = lastApplied
	kv.migrating = migrating
	kv.config = config
	kv.pendingConfig = pendingConfig
	kv.shardVersions = shardVersions
	return 0
}