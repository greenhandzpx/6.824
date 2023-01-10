/*
 * @Author: greenhandzpx 893522573@qq.com
 * @Date: 2023-01-09 22:57:24
 * @LastEditors: greenhandzpx 893522573@qq.com
 * @LastEditTime: 2023-01-10 22:24:11
 * @FilePath: /src/shardkv/server_routine.go
 * @Description: 
 * 
 * Copyright (c) 2023 by greenhandzpx 893522573@qq.com, All Rights Reserved. 
 */
package shardkv

import (
	"bytes"
	"time"
)
import "6.824/labgob"


func (kv *ShardKV) checkCh() {
	for !kv.killed() {
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
				} else if op.Type == GETSHARDS {
					// Get Shards request
					kv.handleGetShards(op, commandMsg.CommandIndex)
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

// checkStateSize
// 检查server的字节数是否接近maxraftstate
func (kv *ShardKV) checkStateSize() {
	for !kv.killed() {
		if kv.maxraftstate == -1 {
			return
		}
		kv.mu.Lock()
		if kv.rf.GetPersister().RaftStateSize() > kv.maxraftstate+200 {
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
			err = e.Encode(kv.migrating)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			err = e.Encode(kv.config)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			err = e.Encode(kv.pendingConfig)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}
			err = e.Encode(kv.shardVersions)
			if err != nil {
				DPrintf("encode error")
				kv.mu.Unlock()
				return
			}

			data := w.Bytes()
			kv.rf.Snapshot(kv.lastApplied, data)
			// DPrintf("%v(%v) calls snapshot, lastApplied:%v", kv.me, kv.gid, kv.lastApplied)
		}
		kv.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// checkConfig
// poll the shard controller to get the latest config
func (kv *ShardKV) checkConfig() {

	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(80 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		configNumNow := kv.config.Num
		kv.mu.Unlock()
		// newConfig := kv.mck.Query(-1)
		// query the next version config
		newConfig := kv.mck.Query(configNumNow + 1)

		op := Op{}
		kv.mu.Lock()
		
		if newConfig.Num > kv.config.Num &&
			time.Now().UnixMilli() - kv.lastMigrateTime > migrateTimeout {
			// migrateTimeout is used to not sending migrate request too often
		// if newConfig.Num > kv.config.Num && newConfig.Num >= kv.configOutstanding {
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.mu.Unlock()
				continue
			}
			kv.lastMigrateTime = time.Now().UnixMilli()
			DPrintf("config has updated, ser:%v(%v), newNum:%v, oldNum:%v",
				kv.me, kv.gid, newConfig.Num, kv.config.Num)
			
			kv.configOutstanding = newConfig.Num
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