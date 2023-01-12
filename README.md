## 6.824

### lab1

### lab2

### lab3

### lab4

#### lab4a(Shard Controller)

跟lab3的思路基本一致，只是存储的是配置信息而不是KV.

#### lab4b(Shard KV)

##### 基本思路

将服务端升级配置以及获取别的group的kv以及发送自己的kv这几个行为都加入到Raft日志中，从而保证同一group的这些行为发生的逻辑顺序完全一致。

##### 实现细节

- 在lab3的基础上，新增若干用于服务端通信的RPC：
    - `GetShards`: 某一Raft组的leader向另一Raft组的leader请求获取某一分片的KV
    - `HandoutShards`: 某一Raft组的leader向另一Raft组的leader发送某一分片的KV

- 新增若干apply层的处理函数：
    - `HandleMigrate`: 每一个group的leader会定期检查配置信息是否需要更新，若需要，则新增`MIGRATE`类型的信息到Raft层中，对于该类型信息的处理，首先将服务端的`migrating`字段置为真，若是leader，则需要向其他group的leader发送`GetShards`RPC，注意`GetShards`只会新增一个`GETSHARDS`类型的信息到Raft层中，然后返回，而不会等该行为完全处理完毕并返回所需KV，这是为了防止死锁。
    - `HandleGetShards`: 当处理另外一个group发来的请求shard的信息时，首先根据其config的版本号来进行相应处理：若args的版本号比自己大很多，则说明自己的config落后很多，给reply填上`ErrRetry`后返回即可; 若args的版本号比自己大一，说明另一个group的版本号跟自己一样，此时若自己的`migrating`字段为真，说明正在升级，那可以安全地将kv发送出去（不安全指同时存在两个group处理同一个shard），若`migrating`字段为假，则需要发送一个`MIGRATE`类型的信息到Raft层中，然后返回`ErrRetry`；其他情况可以安全地把kv发送出去。
    - `HandleHandoutShards`: 当处理另外一个group发来的给予shard的信息时，检查config的有效性后将kv添加到数据库即可；另外需要记录每个shard的版本号，然后每收到一个shard就更新其版本号，并且检查`pendingConfig`(即当前正在迁移的config)中自己所在group需要管理的shard是否都已更新到最新版本号，若是，则将`migrating`字段置为假，完成迁移。

- 修改原来的`HandleGet` `HandlePut` `HandleAppend`函数：
    - 若`migrating`为真，表明正在迁移，返回`ErrWrongGroup`
    - 若key对应的shard不归自己管，返回`ErrWrongGroup`

- 细节
    - 发送kv时需要将每个客户端的唯一标识以及对应的序列号一并发送过去，保证客户端跨组请求的幂等性。