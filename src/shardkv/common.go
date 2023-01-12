/*
 * @Author: greenhandzpx 893522573@qq.com
 * @Date: 2023-01-09 14:38:42
 * @LastEditors: greenhandzpx 893522573@qq.com
 * @LastEditTime: 2023-01-12 15:12:00
 * @FilePath: /src/shardkv/common.go
 * @Description:
 *
 * Copyright (c) 2023 by greenhandzpx 893522573@qq.com, All Rights Reserved.
 */
package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	// when the server that needs to handout shards is far behind
	ErrRetry       = "ErrRetry"  
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 每个客户的唯一标识
	Uuid int64
	// 每个客户发送的请求序号
	Count int
}

type PutAppendReply struct {
	Err      Err
	LeaderId int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err      Err
	Value    string
	LeaderId int
}

type GetShardsArgs struct {
	Shard int
	Config shardctrler.Config
	Gid int
}

type GetShardsReply struct {
	// Kvs map[string]string
	Err Err
	Config shardctrler.Config
}

type HandoutShardsArgs struct {
	Err Err
	Shard int
	Gid int
	Kvs map[string]string
	Config shardctrler.Config
	Ids map[int64]int
}

type HandoutShardsReply struct {
	Err Err
}