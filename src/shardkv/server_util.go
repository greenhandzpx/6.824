package shardkv

import (
	"time"
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


// func (kv *ShardKV) sendHandoutShards