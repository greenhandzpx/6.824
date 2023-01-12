package shardkv

import (
	"time"
)


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
		DPrintf("%v(%v) get req error wrong group, idx:%v because of migrating, pending config num %v", kv.me, kv.gid, commandIndex, kv.pendingConfig.Num)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}

	if kv.config.Shards[key2shard(op.Key)] != kv.gid {
		// this shard isn't controlled by this server
		reply.Err = ErrWrongGroup
		DPrintf("%v(%v) get req error wrong group, idx:%v", kv.me, kv.gid, commandIndex)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}

	value, ok := kv.kvs[op.Key]
	if ok {
		reply.Err = OK
		reply.Value = value
		DPrintf("(idx:%v)%v(%v) get key:%v value:%v, config num %v", commandIndex, kv.me, kv.gid, op.Key, value, kv.config.Num)
	} else {
		DPrintf("(idx:%v)%v(%v) no value for key %v", commandIndex, kv.me, kv.gid, op.Key)
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

	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
		reply.Err = OK
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		kv.sendReply(commandIndex, &reply)
		return 
	}

	if kv.migrating {
		// the server is migrating, should not accept any requests
		reply.Err = ErrWrongGroup
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		DPrintf("%v(%v) append req error wrong group, idx:%v k:%v v:%v because of migrating, pending config num %v", 
			kv.me, kv.gid, commandIndex, op.Key, op.Value, kv.pendingConfig.Num)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}

	if kv.config.Shards[key2shard(op.Key)] != kv.gid {
		// this shard isn't controlled by this server
		reply.Err = ErrWrongGroup
		DPrintf("%v(%v) append req error wrong group, idx:%v k:%v v:%v bec", kv.me, kv.gid, commandIndex, op.Key, op.Value)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}


	// if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
	// 	DPrintf("count %v already exists", op.Count)
	// 	reply.Err = OK
	// } else {
	kv.ids[op.Uuid] = op.Count

	_, ok := kv.kvs[op.Key]
	if ok {
		reply.Err = OK
		kv.kvs[op.Key] += op.Value
		DPrintf("(idx:%v)%v(%v) append key:%v value:%v config num %v", commandIndex, kv.me, kv.gid, op.Key, op.Value, kv.config.Num)
		DPrintf("%v(%v) now k:%v, v:%v", kv.me, kv.gid, op.Key, kv.kvs[op.Key])
	} else {
		reply.Err = ErrNoKey
		kv.kvs[op.Key] = op.Value
		DPrintf("%v(%v) put(append) key:%v value:%v", kv.me, kv.gid, op.Key, op.Value)
	}
	// }

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

	if _, exists := kv.ids[op.Uuid]; exists && kv.ids[op.Uuid] >= op.Count {
		DPrintf("count %v already exists", op.Count)
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		kv.sendReply(commandIndex, &reply)
		return
	}

	if kv.migrating {
		// the server is migrating, should not accept any requests
		reply.Err = ErrWrongGroup
		kv.lastApplied = commandIndex
		//kv.lastApplied++
		DPrintf("%v(%v) put req error wrong group, idx:%v because of migrating, pending config num %v", kv.me, kv.gid, commandIndex, kv.pendingConfig.Num)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}

	if kv.config.Shards[key2shard(op.Key)] != kv.gid {
		// this shard isn't controlled by this server
		reply.Err = ErrWrongGroup
		DPrintf("%v(%v) put req error wrong group, idx:%v", kv.me, kv.gid, commandIndex)
		kv.lastApplied = commandIndex
		kv.sendReply(commandIndex, &reply)
		return
	}


	kv.ids[op.Uuid] = op.Count


	DPrintf("(idx:%v)%v(%v) put key:%v value:%v config num %v", commandIndex, kv.me, kv.gid, op.Key, op.Value, kv.config.Num)
	kv.ids[op.Uuid] = op.Count
	kv.kvs[op.Key] = op.Value
	// }
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

	// if op.Conf.Num <= kv.config.Num || op.Conf.Num < kv.configOutstanding {
	if op.Conf.Num <= kv.config.Num {
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
		// return
	}

	DPrintf("%v(%v) get a migrate request %v", kv.me, kv.gid, commandIndex)

	// handoutKvs := make(map[string]string)
	kv.pendingConfig = op.Conf

	if kv.config.Num == 0 {
		DPrintf("%v(%v) first conf", kv.me, kv.gid)
		// means this server just started
		// then just finish the migration
		for shard, gid := range op.Conf.Shards {
			if gid == kv.gid {
				kv.shardVersions[shard] = op.Conf.Num
			}
		}
		kv.migrating = false
		kv.config = op.Conf
		kv.configOutstanding = 0
		kv.pendingConfig.Num = 0
		return
	}


	// if _, isLeader := kv.rf.GetState(); !isLeader {
	// 	// only leader fetch shards from other group
	// 	return
	// }

	// update the shards set
	shouldGetNewShard := false
	for shard, gid := range op.Conf.Shards {
		if kv.gid != gid {
			continue
		}
		if kv.config.Shards[shard] == gid {
			kv.shardVersions[shard] = op.Conf.Num
			continue
		}

		shouldGetNewShard = true

		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		if kv.shardVersions[shard] >= op.Conf.Num {
			// this shard has been latest
			continue
		}

		DPrintf("%v(%v) should have a new shard %v, gid %v",
			kv.me, kv.gid, shard, kv.config.Shards[shard])


		args := GetShardsArgs{
			Shard: shard,
			Config: op.Conf,
			Gid: kv.gid,
		}
		targetGid := kv.config.Shards[shard]
		DPrintf("%v(%v) fetch shards from group %v",
			kv.me, kv.gid, targetGid)
		// only the leader sends rpc request to other groups
		// to fetch shards
		kv.mu.Unlock()
		for {
			success := false
			for _, srv := range kv.config.Groups[targetGid] {
				server := kv.make_end(srv)
				var replyTmp GetShardsReply

				// DPrintf("fetch shard before")
				ok := server.Call("ShardKV.GetShards", &args, &replyTmp)
				// DPrintf("fetch shard after")

				if ok && replyTmp.Err == OK {
					success = true
					// for k, v := range replyTmp.Kvs {
					// 	handoutKvs[k] = v
					// }
					break
				}
				// if ok && replyTmp.Err == ErrWrongGroup {
				// 	// the server in other group reject,
				// 	// which means this group's config may be outdated
				// 	if kv.config.Num < replyTmp.Config.Num {
				// 		// update again
				// 		configChangedOp := Op {}
				// 		configChangedOp.Type = MIGRATE
				// 		configChangedOp.Conf = replyTmp.Config
				// 		kv.rf.Start(configChangedOp)
				// 		brea
				// 	}
				// }
			}
			if success {
				break
			}
		}
		kv.mu.Lock()
	}

	if !shouldGetNewShard {
		// the new config has no new shard that we should get
		// which means we don't send any rpc to other group to get shard
		// then just finish the migration
		kv.migrating = false
		kv.config = op.Conf
		kv.configOutstanding = 0
		kv.pendingConfig.Num = 0
		DPrintf("%v(%v) finish migrate, conf num %v", kv.me, kv.gid, kv.config.Num)
	}
	// // When we fetch all required shards, we send them to the raft
	// // and let all the other servers get those shards.
	// DPrintf("%v(%v) finish fetching, start handout", kv.me, kv.gid)
	// kv.lastApplied = commandIndex
	// newOp := Op{
	// 	Type: HANDOUTSHARDS,
	// 	Conf: op.Conf,
	// 	Kvs:  handoutKvs,
	// }
	// kv.rf.Start(newOp)

	// only when we get the raft shards, will we update the config
	//kv.config = op.Conf
}

func (kv *ShardKV) handleGetShards(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, isLeader := kv.rf.GetState(); !isLeader {
		DPrintf("%v(%v isn't leader, shouldn't handle getShards", kv.me, kv.gid)
		return
	}

	handoutArgs := HandoutShardsArgs {
		Err: OK,
		Shard: op.Shard,
		Gid: kv.gid,
		Config: op.Conf,
	}


	if op.Conf.Num > kv.config.Num + 1 {
		// our config is too far behind
		handoutArgs.Err = ErrRetry

	} else {
		// op.Conf.Num <= kv.config.Num + 1
		if !kv.migrating &&
			op.Conf.Num == kv.config.Num + 1 {
			// update our own config
			DPrintf("%v(%v) should update config(now %v) when got get_shard(shard %v)request", kv.me, kv.gid,  kv.config.Num, op.Shard)
			kv.lastMigrateTime = time.Now().UnixMilli()
			migrateOp := Op {
				Type: MIGRATE,
				Conf: op.Conf,
			}
			kv.rf.Start(migrateOp)
			// tell the other group to retry
			handoutArgs.Err = ErrRetry
		} else {
			// handout our shard
			cnt := 0
			handoutArgs.Kvs = make(map[string]string)
			for k, v := range kv.kvs {
				if key2shard(k) == op.Shard {
					handoutArgs.Kvs[k] = v
					cnt++
				}
			}
			// also need to transfer the client uuids
			handoutArgs.Ids = make(map[int64]int)
			for id, cnt := range kv.ids {
				handoutArgs.Ids[id] = cnt
			}
			DPrintf("%v(%v) send handout shard %v to group %v, kv cnt %v ids cnt %v", kv.me, kv.gid, op.Shard, 
					op.Conf.Shards[op.Shard], cnt, len(handoutArgs.Ids))	
		}
	}

	// Then send back the handout op and 
	// put into that server's raft log
	kv.mu.Unlock()
	DPrintf("%v(%v) send back handout shard %v to group %v", kv.me, kv.gid, op.Shard, 
			op.Conf.Shards[op.Shard])	
	for {
		success := false
		for _, srv := range op.Conf.Groups[op.Conf.Shards[op.Shard]] {
			server := kv.make_end(srv)
			var replyTmp HandoutShardsReply

			ok := server.Call("ShardKV.HandoutShards", &handoutArgs, &replyTmp) 
			if ok && replyTmp.Err == OK {
				success = true
				break
			}
		}
		if success {
			break
		}
	}

	kv.mu.Lock()


}

func (kv *ShardKV) handleHandoutShards(op Op, commandIndex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if commandIndex <= kv.lastApplied {
		DPrintf("outdated command in %v(%v)", kv.me, kv.gid)
		return
	}

	// if op.Conf.Num <= kv.config.Num || op.Conf.Num < kv.configOutstanding {
	if op.Conf.Num <= kv.config.Num {
		// outdated conf
		DPrintf("outdated handout shard %v config num %v in %v(%v)", op.Shard, op.Conf.Num, kv.me, kv.gid)
		return
	}

	if op.Conf.Num != kv.pendingConfig.Num {
		// outdated conf
		DPrintf("outdated handout shard %v config num %v in %v(%v)", op.Shard, op.Conf.Num, kv.me, kv.gid)
		return
	}

	if op.Conf.Num <= kv.shardVersions[op.Shard] {
		// outdated shard
		DPrintf("outdated handout shard %v config num %v in %v(%v)", op.Shard, op.Conf.Num, kv.me, kv.gid)
		return
	}

	DPrintf("%v(%v) handle handout shards, shard %v conf num %v ids size %v", kv.me, kv.gid, op.Shard, op.Conf.Num, len(op.Ids))

	if op.HandoutErr == ErrRetry {	
		// ErrRetry means the src group is updating its config and we need to 
		// request again
		DPrintf("%v(%v) get handout shard %v but should retry", kv.me, kv.gid, op.Shard)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		args := GetShardsArgs {
			Shard: op.Shard,
			Config: kv.pendingConfig,
			Gid: kv.gid,
		}
		kv.mu.Unlock()
		for {
			success := false
			for _, srv := range kv.config.Groups[kv.config.Shards[op.Shard]] {
				server := kv.make_end(srv)
				var replyTmp GetShardsReply

				ok := server.Call("ShardKV.GetShards", &args, &replyTmp)

				if ok && replyTmp.Err == OK {
					success = true
					break
				}
			}
			if success {
				break
			}
		}
		kv.mu.Lock()
		return
	}

	// add these kvs to the kv database
	for k, v := range op.Kvs {
		kv.kvs[k] = v
	}
	// add ids to the kv ids
	for id, cnt := range op.Ids {
		if _, exists := kv.ids[id]; exists {
			if kv.ids[id] < cnt {
				kv.ids[id] = cnt
			}
		} else {
			kv.ids[id] = cnt
		} 
	}
	kv.shardVersions[op.Shard] = op.Conf.Num

	// check whether all shards have been latest
	migrateFinished := true
	for shard, gid := range kv.pendingConfig.Shards {
		if gid != kv.gid {
			continue
		}
		if kv.shardVersions[shard] < kv.pendingConfig.Num {
			migrateFinished = false
			DPrintf("%v(%v) didn't finish migrate yet, conf num %v, miss shard %v", kv.me, kv.gid, kv.config.Num, shard)
			// break
		}
	}
	if migrateFinished {
		kv.config = op.Conf
		kv.migrating = false
		kv.configOutstanding = 0
		kv.pendingConfig.Num = 0
		DPrintf("%v(%v) finish migrate, conf num %v", kv.me, kv.gid, kv.config.Num)
	}
	// kv.config = op.Conf
	// kv.migrating = false
	// kv.configOutstanding = 0
}