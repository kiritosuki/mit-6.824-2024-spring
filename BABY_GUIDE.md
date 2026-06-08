# BABY_GUIDE.md —— 代码逐行讲解

> 本文档逐文件、逐函数讲解 Lab 3（Raft）和 Lab 4（KV Raft）的全部实现代码。只讲解我写的部分，框架代码仅做简要说明。建议配合源码阅读。

---

## 目录

- [Part A: Raft 共识算法 (src/raft)](#part-a-raft-共识算法-srcraft)
  - [raft.go 完整讲解](#raftgo-完整讲解)
    - [常量定义](#常量定义)
    - [数据结构](#数据结构)
    - [工具函数](#工具函数)
    - [持久化](#持久化)
    - [快照](#快照)
    - [RequestVote —— 请求投票](#requestvote--请求投票)
    - [选举逻辑](#选举逻辑)
    - [Start —— 提交命令](#start--提交命令)
    - [AppendEntries —— 日志复制](#appendentries--日志复制)
    - [Replicator —— 复制协程](#replicator--复制协程)
    - [Applier —— 应用协程](#applier--应用协程)
    - [InstallSnapshot —— 快照安装](#installsnapshot--快照安装)
    - [Ticker —— 定时器](#ticker--定时器)
    - [Make —— 初始化](#make--初始化)
    - [RaftRPC 接口](#raftrpc-接口)
- [Part B: KV Raft 容错键值服务 (src/kvraft)](#part-b-kv-raft-容错键值服务-srckvraft)
  - [common.go](#commongo)
  - [server.go](#servergo)
  - [client.go](#clientgo)

---

## Part A: Raft 共识算法 (src/raft)

### raft.go 完整讲解

#### 常量定义

```go
const (
    Leader             = 1
    Candidate          = 2
    Follower           = 3
    HeartBeatGap       = 125        // 心跳间隔 125ms
    ElectionTimeoutMin = 300        // 最小选举超时 300ms
    ElectionTimeoutMax = 500        // 最大选举超时 500ms
)
```

三个角色常量和时间参数。**心跳间隔必须远小于选举超时**（125ms vs 300-500ms），否则 Follower 会频繁超时发起不必要的选举。

#### 数据结构

**ApplyMsg**（raft.go:52-63）：

```go
type ApplyMsg struct {
    CommandValid bool          // true: 这是一条要执行的命令
    Command      interface{}   // 命令内容
    CommandIndex int           // 日志 index
    CommandTerm  int           // 日志 term

    // 快照相关 (Lab 3D)
    SnapshotValid bool         // true: 这是一个快照
    Snapshot      []byte       // 快照数据
    SnapshotTerm  int          // 快照对应的 term
    SnapshotIndex int          // 快照对应的最后一个 index
}
```

这是 Raft 向上层服务（KV Server 或 Tester）发送消息的 channel。有两种消息类型：
- **CommandValid=true**：一条已提交的日志条目，上层需要执行
- **SnapshotValid=true**：一个快照，上层需要用快照替换状态机

**Raft 结构体**（raft.go:66-99）：

```go
type Raft struct {
    mu        sync.Mutex          // 全局锁，保护所有共享状态
    peers     []*labrpc.ClientEnd // 所有 peer 的 RPC 端点
    persister *Persister          // 持久化存储接口
    me        int                 // 自己在 peers[] 中的索引
    dead      int32               // Kill() 标志位

    lastHeartBeat time.Time       // 最近收到心跳/投票的时间
    applyCh       chan ApplyMsg   // 向上层发送 ApplyMsg 的 channel

    replicatorCond []*sync.Cond   // 每个 peer 一个，唤醒 replicator
    applierCond    *sync.Cond     // 唤醒 applier

    stat int                      // 当前角色

    // === 持久化状态（所有 server） ===
    currentTerm int
    voteFor     int               // -1 表示没投票
    log         []LogEntry        // index 0 是 dummy/snapshot 占位符

    // === 易失状态（所有 server） ===
    commitIndex int               // 已提交的最大 index
    lastApplied int               // 已应用到状态机的最大 index

    // === 易失状态（仅 Leader） ===
    nextIndex  []int              // 下一条要发给每个 peer 的 index
    matchIndex []int              // 每个 peer 已匹配的最大 index

    // === 快照传递 ===
    msg *ApplyMsg                 // 待发送的快照 ApplyMsg（由 InstallSnapshot 设置，applier 发送）
}
```

**LogEntry**（raft.go:101-105）：

```go
type LogEntry struct {
    Term    int
    Index   int
    Command interface{}
}
```

每条日志有 Term（哪个任期产生的）、Index（全局唯一递增位置）、Command（实际命令）。注意 `Index` 是显式存储的，因为在快照后 `log[0].Index` 可能远大于 1。

---

#### 工具函数

**toRealIndex**（raft.go:108-110）：

```go
// 确保调用时已持锁
func (rf *Raft) toRealIndex(index int) int {
    return index - rf.log[0].Index
}
```

**这是整个 Raft 实现中最基础也最重要的辅助函数！**

由于快照会截断日志（删除 `log[0]` 之前的所有条目），物理数组下标和逻辑 index 不再一致。`log[0]` 的 Index 可能从 0 变为 100、200…… `toRealIndex` 将逻辑 index 转换为数组下标。

例如：`log[0].Index = 100`，要访问 `index=105` 的日志 → `toRealIndex(105) = 5` → `log[5]`。

**GetState**（raft.go:114-118）：

```go
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.stat == Leader
}
```

给上层服务查询当前节点的 term 和是否是 Leader。

---

#### 持久化

**encodeState**（raft.go:120-127）：

```go
func (rf *Raft) encodeState() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.voteFor)
    e.Encode(rf.log)
    return w.Bytes()
}
```

将三个持久化状态（currentTerm、voteFor、log）用 Go 的 gob 编码器序列化为字节数组。

**persist**（raft.go:137-146）：

```go
// 确保调用时已持锁
func (rf *Raft) persist() {
    persistState := rf.encodeState()
    snapshot := rf.persister.ReadSnapshot()
    if snapshot != nil && len(snapshot) != 0 {
        rf.persister.Save(persistState, snapshot)
    } else {
        rf.persister.Save(persistState, nil)
    }
}
```

将 Raft 状态和快照一起原子保存。这里先读取已有快照再一起保存的原因：`Persister.Save` 要求 raft state 和 snapshot 作为一个整体写入，如果只传 raft state 不传 snapshot，旧快照就丢失了。

**readPersist**（raft.go:150-171）：

```go
// 确保调用时已持锁
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var voteFor int
    var log []LogEntry
    if d.Decode(&currentTerm) != nil ||
        d.Decode(&voteFor) != nil ||
        d.Decode(&log) != nil {
        fmt.Println("[error] can't read persist state")
    } else {
        rf.currentTerm = currentTerm
        rf.voteFor = voteFor
        rf.log = log
        rf.commitIndex = log[0].Index
        rf.lastApplied = log[0].Index
    }
}
```

从持久化存储恢复状态。反序列化出 currentTerm、voteFor、log 三个字段。

**关键行**：
```go
rf.commitIndex = log[0].Index
rf.lastApplied = log[0].Index
```

如果是正常重启（无快照），`log[0].Index = 0`，所以 `commitIndex = lastApplied = 0`。
如果是从快照恢复，`log[0]` 是快照的 `LastIncludedIndex`，比如 100，那么 `commitIndex = lastApplied = 100`，不会重复 apply 快照包含的日志。

---

#### 快照

**Snapshot**（raft.go:177-188）：

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // 拒绝过时的快照
    if index <= rf.log[0].Index {
        return
    }

    // 截断日志：保留 index 及之后的部分
    rf.log = append([]LogEntry{}, rf.log[rf.toRealIndex(index):]...)
    rf.log[0].Command = nil  // 第一条成为占位符

    // 持久化
    rf.persister.Save(rf.encodeState(), snapshot)
}
```

**上层服务（如 KV Server）调用此函数来做快照**。关键点：

1. `index <= rf.log[0].Index`：拒绝旧快照。比如 `log[0].Index` 已经是 200（已有快照覆盖到 200），但又收到一个覆盖到 150 的快照——这是过时的。

2. `append([]LogEntry{}, ...)` 创建新 slice：Go 的 slice 操作是共享底层数组的。如果直接用 `rf.log = rf.log[toRealIndex(index):]`，底层数组可能仍持有旧日志的引用，导致 GC 无法回收。创建新 slice 可以释放旧数组。

3. `rf.log[0].Command = nil`：第一条成为"快照占位符"，表示这是一个虚拟条目。它的 `Index` 是 `LastIncludedIndex`，`Term` 是 `LastIncludedTerm`，但 `Command` 为 nil 表示它不应该被 apply。

---

#### RequestVote —— 请求投票

**RequestVoteArgs**（raft.go:192-198）和 **RequestVoteReply**（raft.go:202-206）：

```go
type RequestVoteArgs struct {
    Term         int   // Candidate 的 term
    CandidateId  int   // Candidate 的 id
    LastLogIndex int   // Candidate 最后一条日志的 index
    LastLogTerm  int   // Candidate 最后一条日志的 term
}

type RequestVoteReply struct {
    Term        int   // 投票者的 term（让 Candidate 知道自己是否过时）
    VoteGranted bool  // 是否投票
}
```

**RequestVote Handler**（raft.go:209-226）：

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()   // ⚠️ 函数返回前持久化（可能修改了 voteFor）

    reply.VoteGranted = false  // 默认拒绝

    // 第一步：term 检查
    if !rf.checkRequestTerm(args, reply) {
        return
    }

    // 第二步：投票条件判断
    if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isUpToDate(args) {
        reply.VoteGranted = true
        rf.voteFor = args.CandidateId   // 记录投票
        rf.lastHeartBeat = time.Now()   // 重置超时计时器
    }
}
```

**逐行分析**：

1. `defer rf.persist()`：因为 `rf.voteFor` 可能被修改，需要持久化。用 defer 保证锁释放前刷盘。

2. `reply.VoteGranted = false`：默认不投票。只有满足所有条件才改为 true。

3. `checkRequestTerm(args, reply)`：检查 term
   - 如果 `args.Term < rf.currentTerm`：Candidate 是旧 Leader/旧 Candidate，拒绝，返回 false
   - 如果 `args.Term > rf.currentTerm`：发现自己过时了，更新 term 并降级为 Follower，继续处理
   - `reply.SetTerm(rf.currentTerm)` 在 defer 中执行，告知 Candidate 当前 term

4. `rf.voteFor == -1 || rf.voteFor == args.CandidateId`：
   - `-1`：这个 term 还没投过票
   - 等于这个 Candidate：这个 term 已经投过给这个候选人（RPC 重发的情况）

5. `rf.isUpToDate(args)`：检查 Candidate 的日志是否足够新

**isUpToDate**（raft.go:228-233）：

```go
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
    lastLog := rf.log[len(rf.log)-1]
    candidateIndex := args.LastLogIndex
    candidateTerm := args.LastLogTerm
    return candidateTerm > lastLog.Term ||
        (candidateTerm == lastLog.Term && candidateIndex >= lastLog.Index)
}
```

**比较规则**：
- 谁的 LastLogTerm 更大谁更新
- Term 相同，谁的 Index 更大谁更新

**为什么这样比较？** 因为任期更大的日志一定更新（Leader 只有在自己的 Term 中才能提交日志，term 严格单调递增）。同任期内的 Index 越大越新。

**sendRequestVote**（raft.go:262-265）：

```go
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}
```

只是对 `labrpc.Call` 的封装。这个函数在 `doRequestVote` 中没有被使用（`doRequestVote` 直接调用了 `rf.peers[server].Call`），但保留它是为了符合接口约定。

---

#### 选举逻辑

**launchElection**（raft.go:304-323）：

```go
func (rf *Raft) launchElection() {
    rf.currentTerm++           // 任期+1
    rf.stat = Candidate        // 变为 Candidate
    rf.voteFor = rf.me         // 投自己一票
    rf.lastHeartBeat = time.Now()
    lastLog := rf.log[len(rf.log)-1]
    voteCount := int32(1)      // 已有一票（自己）
    args := RequestVoteArgs{
        Term:         rf.currentTerm,
        CandidateId:  rf.me,
        LastLogIndex: lastLog.Index,
        LastLogTerm:  lastLog.Term,
    }
    for id := range rf.peers {
        if id == rf.me {
            continue
        }
        go rf.doRequestVote(id, &args, &voteCount)
    }
}
```

**步骤拆解**：

1. `currentTerm++`：每次选举用新 term。如果不递增，一个旧 Candidate 可能会干扰正在运行的集群（集群已经在更高的 term 了）。

2. `stat = Candidate`，`voteFor = me`：转变角色，先投自己一票。

3. `voteCount := int32(1)`：用 `int32` 而不是 `int`，因为要传给 `atomic.AddInt32`。初始值为 1（自己的一票）。

4. **并发向所有 peer 发送 RequestVote**：用 `go` 启动 goroutine，并传入 `&voteCount` 的指针，让所有 goroutine 共享同一个计数器。

**doRequestVote**（raft.go:267-302）：

```go
func (rf *Raft) doRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
    reply := &RequestVoteReply{}
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    if !ok {
        return  // RPC 失败（网络问题、server 挂了等），放弃
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    // 检查回复的 term
    if !rf.checkResponseTerm(args, reply, true) {
        return  // 对方 term 更高，我已经过期了
    }

    if !reply.VoteGranted {
        return  // 没拿到投票
    }

    // 拿到一票！原子地增加计数
    if atomic.AddInt32(voteCount, 1) > int32(len(rf.peers)/2) &&
        rf.stat == Candidate &&
        rf.currentTerm == args.Term {

        rf.stat = Leader  // 🎉 成为 Leader！
        lastLogIndex := rf.log[len(rf.log)-1].Index

        // 初始化 Leader 状态
        for i := range rf.peers {
            rf.nextIndex[i] = lastLogIndex + 1
            rf.matchIndex[i] = 0
        }

        // 立即向所有 peer 发心跳（空的 AppendEntries），巩固 Leader 地位
        for i := range rf.peers {
            if i != rf.me {
                args2 := rf.prepareReplicationArgs(i)
                go rf.doReplicate(i, args2)
            }
        }
    }
}
```

**关键细节**：

1. **`atomic.AddInt32(voteCount, 1)`**：多个 goroutine 并发修同一个计数器，必须用原子操作。

2. **过半判断**：`voteCount > len(rf.peers)/2`
   - 3 个节点：`> 1`，需要 2 票
   - 5 个节点：`> 2`，需要 3 票
   - 整数除法会截断：`5/2 = 2`，`> 2` 即至少 3

3. **双重确认**：`rf.stat == Candidate && rf.currentTerm == args.Term`
   - 确保在收集投票的过程中，自己没有降级为 Follower（可能收到了更高 term 的心跳）
   - 确保当前 term 和发起选举时一致（可能已经被其他事件改变了）

4. **`nextIndex` 初始化**：全部设为 `lastLogIndex + 1`——乐观假设所有 Follower 日志和自己一样。如果 Follower 落后，第一次 AppendEntries 会失败，Leader 再调整（快速回溯）。

5. **立即发心跳**：防止其他 Follower 超时发起新一轮选举。

---

#### Start —— 提交命令

```go
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.stat != Leader {
        return -1, -1, false  // 不是 Leader，拒绝
    }
    lastIndex := rf.log[len(rf.log)-1].Index
    rf.log = append(rf.log, LogEntry{
        Term:    rf.currentTerm,
        Index:   lastIndex + 1,
        Command: command,
    })
    // 唤醒所有 replicator
    for i := range rf.peers {
        if i != rf.me {
            rf.replicatorCond[i].Signal()
        }
    }
    return lastIndex + 1, rf.currentTerm, true
}
```

**流程**：
1. 检查是否是 Leader，不是则返回 `false`
2. 计算下一个 index（最后一条日志 index + 1）
3. 追加到本地日志
4. **逐个 Signal 每个 replicator**：唤醒所有复制协程，让它们向各自的 Follower 发送新的日志

**返回值含义**：
- `index`：这条命令将来被 commit 时的位置
- `term`：当前任期
- `isLeader`：调用者是不是 Leader

注意：**Start 只保证命令被追加到 Leader 本地日志，不保证最终会被 commit**。如果 Leader 在 commit 前崩溃，这条日志可能永远丢失。

---

#### AppendEntries —— 日志复制

**参数和回复结构**（raft.go:325-340）：

```go
type AppendEntriesArgs struct {
    Term         int         // Leader 的 term
    LeaderId     int         // Leader 的 id
    PrevLogIndex int         // 新日志前一条的 index
    PrevLogTerm  int         // 新日志前一条的 term
    Entries      []LogEntry  // 要复制的日志（心跳时为空）
    LeaderCommit int         // Leader 的 commitIndex
}

type AppendEntriesReply struct {
    Term          int   // Follower 的 term
    Success       bool  // 是否成功
    ConflictIndex int   // 冲突 index（快速回溯用）
    ConflictTerm  int   // 冲突 term（快速回溯用）
    LogLen        int   // Follower 日志长度（快速回溯用）
}
```

**prepareReplicationArgs**（raft.go:389-412）：

```go
// 确保调用时已持锁
func (rf *Raft) prepareReplicationArgs(server int) interface{} {
    if rf.nextIndex[server] > rf.log[0].Index {
        // 正常情况：Follower 需要的日志还在，发 AppendEntries
        realNextIndex := rf.toRealIndex(rf.nextIndex[server])
        entries := make([]LogEntry, len(rf.log[realNextIndex:]))
        copy(entries, rf.log[realNextIndex:])
        prevLogEntry := rf.log[realNextIndex-1]
        return AppendEntriesArgs{
            Term:         rf.currentTerm,
            LeaderId:     rf.me,
            PrevLogIndex: prevLogEntry.Index,
            PrevLogTerm:  prevLogEntry.Term,
            Entries:      entries,
            LeaderCommit: rf.commitIndex,
        }
    } else {
        // 特殊：Follower 需要的日志已经被快照截断了，发 InstallSnapshot
        return InstallSnapshotArgs{
            Term:              rf.currentTerm,
            LeaderId:          rf.me,
            LastIncludedIndex: rf.log[0].Index,
            LastIncludedTerm:  rf.log[0].Term,
            Data:              rf.persister.ReadSnapshot(),
        }
    }
}
```

**两种路径**：
- `nextIndex[server] > log[0].Index`：正常发 AppendEntries，带上从 `nextIndex` 开始的所有日志
- `nextIndex[server] <= log[0].Index`：Follower 要的日志已经被快照截断了，必须发整个快照

复制 entries 时用 `copy` 而不是 slice 赋值：防止并发修改导致数据竞争。

**AppendEntries 参数构建逻辑**：
- `PrevLogIndex` 和 `PrevLogTerm` 是 `nextIndex - 1` 处的日志信息。Follower 用这两个字段做一致性检查（"我 PrevLogIndex 处的 Term 是不是 PrevLogTerm？"）。
- `Entries` 是从 `nextIndex` 开始的所有日志。心跳时这个列表为空。

**AppendEntries Handler**（raft.go:552-611）：

这是 Raft 最长的函数，也是核心。我分段讲解：

**第一段：Term 检查和角色重置**：

```go
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    // Term 检查
    if !rf.checkRequestTerm(args, reply) {
        return
    }

    // 如果我是 Candidate，但收到了合法 Leader 的心跳 → 认怂，降级
    if rf.stat == Candidate {
        rf.stat = Follower
    }

    // 重置选举超时
    rf.lastHeartBeat = time.Now()
```

- 收到 AppendEntries 意味着有一个合法的 Leader 存在。如果当前是 Candidate，说明选举已经结束了（有人赢了），自动降级。
- 重置 `lastHeartBeat` = 刚才收到了 Leader 的心跳，不要发起选举。

**第二段：一致性检查**：

```go
    realPrevIndex := rf.toRealIndex(args.PrevLogIndex)

    // Case 1: PrevLogIndex 在快照之前
    if realPrevIndex < 0 {
        reply.Success = false
        reply.ConflictTerm = -2                      // 特殊标记
        reply.ConflictIndex = rf.log[0].Index + 1    // 从快照后第一条开始
        return
    }

    // Case 2: PrevLogIndex 超出了我的日志长度
    if realPrevIndex >= len(rf.log) {
        reply.Success = false
        reply.ConflictTerm = -1                      // 特殊标记
        reply.LogLen = rf.log[len(rf.log)-1].Index + 1  // 我的日志末尾+1
        return
    }

    // Case 3: PrevLogIndex 处 Term 不匹配
    if rf.log[realPrevIndex].Term != args.PrevLogTerm {
        reply.Success = false
        reply.ConflictTerm = rf.log[realPrevIndex].Term
        // 找该 ConflictTerm 的第一个 index
        realConflictIndex := 0
        for i := realPrevIndex - 1; i >= 0; i-- {
            if rf.log[i].Term != rf.log[realPrevIndex].Term {
                realConflictIndex = i + 1
                break
            }
        }
        reply.ConflictIndex = rf.log[realConflictIndex].Index
        return
    }
```

**三种失败情况的处理**：

| 情况 | ConflictTerm | 附加信息 | Leader 行为 |
|---|---|---|---|
| Case 1: PrevLogIndex 已被快照截断 | -2 | ConflictIndex=log[0].Index+1 | 下次从 log[0].Index+1 发 InstallSnapshot |
| Case 2: Follower 日志太短 | -1 | LogLen=follower 日志末尾 | 下次跳到 follower 的日志末尾 |
| Case 3: 该位置 term 不匹配 | ≥0 (实际 term 值) | ConflictIndex=该 term 第一条 index | 在自己的日志里找这个 term，跳过整个 term |

**Case 3 中 `ConflictIndex` 为什么要找该 term 的第一条 index？**

假设 Follower 在 index=10 处 Term=4，Leader 在 index=10 处 Term=5。Follower 返回 `ConflictTerm=4, ConflictIndex=4 这个 term 的第一条 index`。Leader 收到后：
1. 在自己的日志里找 Term=4 的最后一条位置（比如 index=8）
2. 下次从 index=9 开始发送

这样跳过了整个冲突的 term，比每次减 1 快速得多。

**第三段：日志合并**：

```go
    // 一致性检查通过！现在合并日志
    for idx, entry := range args.Entries {
        logIndex := rf.toRealIndex(entry.Index)
        if logIndex >= len(rf.log) || rf.log[logIndex].Term != entry.Term {
            // 找到第一个不匹配的位置，截断并追加
            rf.log = append([]LogEntry{}, append(rf.log[:logIndex], args.Entries[idx:]...)...)
            break
        }
    }

    reply.Success = true
```

**合并逻辑**：
- 逐条对比 `args.Entries` 和本地日志
- 找到第一个不匹配的位置（index 超出 / term 不同）
- 截断本地日志，从该位置开始用 Leader 的 entries 覆盖
- `append([]LogEntry{}, ...)` 创建新 slice，释放旧底层数组

**第四段：更新 commitIndex**：

```go
    // 更新 commitIndex
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = args.LeaderCommit
        // 但不能超过自己日志的长度！
        if rf.log[len(rf.log)-1].Index < args.LeaderCommit {
            rf.commitIndex = rf.log[len(rf.log)-1].Index
        }
    }
    rf.applierCond.Signal()  // 唤醒 applier
}
```

Follower 的 commitIndex 不能超过它拥有的最后一条日志的 index。如果 Leader 告诉 Follower "commit 到 100"，但 Follower 的日志只到 95，那只能 commit 到 95。等后续 AppendEntries 带来更多日志后，commitIndex 自然跟上。

**doAppendEntries**（raft.go:479-545）：

这是 Leader 端发完 AppendEntries 后处理回复的函数：

```go
func (rf *Raft) doAppendEntries(server int, args *AppendEntriesArgs) {
    reply := &AppendEntriesReply{}
    ok := rf.sendAppendEntries(server, args, reply)
    if !ok {
        return
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.persist()

    if !rf.checkResponseTerm(args, reply, false) {
        return
    }
```

**成功路径**：

```go
    if reply.Success {
        if len(args.Entries) > 0 {
            rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
        }
        rf.matchIndex[server] = rf.nextIndex[server] - 1

        // 尝试推进 commitIndex
        // 从后往前遍历，找可以 commit 的最大 index
        for n := rf.log[len(rf.log)-1].Index; n > rf.commitIndex && n >= rf.log[0].Index; n-- {
            index := n
            // ⚠️ 关键约束：只能 commit 当前 term 的日志
            if rf.log[rf.toRealIndex(index)].Term != rf.currentTerm {
                continue
            }
            count := 1
            for i := range rf.peers {
                if i != rf.me && rf.matchIndex[i] >= index {
                    count++
                }
            }
            if count > len(rf.peers)/2 && rf.log[rf.toRealIndex(index)].Term == rf.currentTerm {
                rf.commitIndex = index
                break
            }
        }
    }
```

**Commit 检查的要点**：
1. 从最大 index 往前扫描（先尝试 commit 最新的日志）
2. 只考虑**当前 term** 的日志（Raft 论文 Figure 8 的关键安全规则）
3. 过半 `matchIndex[i] >= index`（包括 Leader 自己，所以 count 从 1 开始）
4. 找到第一个满足条件的就 break（因为我们从大到小扫，第一个就是最大的）

**失败路径 —— 快速回溯**：

```go
    } else {
        // 快速回溯优化
        if reply.ConflictTerm == -2 {
            // Follower 日志已被快照截断
            rf.nextIndex[server] = reply.ConflictIndex
        } else if reply.ConflictTerm == -1 {
            // Follower 日志太短
            rf.nextIndex[server] = reply.LogLen
        } else {
            // Term 不匹配：在 Leader 日志中找该 term 的最后一条
            lastIndexOfTerm := -1
            for i := rf.log[len(rf.log)-1].Index; i >= rf.log[0].Index; i-- {
                if rf.log[rf.toRealIndex(i)].Term == reply.ConflictTerm {
                    lastIndexOfTerm = i
                    break
                }
                if rf.log[rf.toRealIndex(i)].Term < reply.ConflictTerm {
                    break  // term 已经小于了，不可能再匹配
                }
            }
            if lastIndexOfTerm == -1 {
                rf.nextIndex[server] = reply.ConflictIndex
            } else {
                rf.nextIndex[server] = lastIndexOfTerm + 1
            }
        }
        if rf.nextIndex[server] < 1 {
            rf.nextIndex[server] = 1
        }
    }
    rf.applierCond.Signal()
}
```

**回溯策略总结**：
- `ConflictTerm == -2`：跳到 `follower.log[0].Index + 1`——准备发快照
- `ConflictTerm == -1`：跳到 follower 日志末尾——发更少的日志
- `ConflictTerm >= 0`：在 Leader 日志中二分查找该 term 的最后一条——跳过整个冲突 term

---

#### Replicator —— 复制协程

```go
func (rf *Raft) replicator(server int) {
    rf.replicatorCond[server].L.Lock()
    defer rf.replicatorCond[server].L.Unlock()
    for !rf.killed() {
        rf.mu.Lock()
        // 等待条件：我是 Leader 且该 peer 的日志需要更新
        for !(rf.stat == Leader && rf.matchIndex[server] < rf.log[len(rf.log)-1].Index) {
            rf.mu.Unlock()
            rf.replicatorCond[server].Wait()
            rf.mu.Lock()
        }
        args := rf.prepareReplicationArgs(server)
        rf.mu.Unlock()
        rf.doReplicate(server, args)
    }
}
```

**设计分析**：

1. **条件变量等待**：replicator 在以下条件成立时才工作：
   - 当前是 Leader
   - 该 peer 的 `matchIndex` 落后于 Leader 的最后一条日志
   - 如果不满足（比如不是 Leader，或者日志已经同步），就 Wait

2. **双锁模式**：
   - 外层条件变量锁：保护 Wait/Signal 语义
   - 内层 `rf.mu`：保护 Raft 共享状态
   - 在进入 Wait 前释放 `rf.mu`，在被唤醒后重新获取 `rf.mu`
   - **为什么不用 `rf.mu` 作为条件变量锁？** 因为 `cond.Wait()` 会释放锁，但如果多个 replicator 共用一个条件变量和锁：
     - 如果共用一个 cond：Signal 只能唤醒一个 replicator，而 Start 需要唤醒所有
     - 如果共用 `rf.mu`：Signal 时需要持有 `rf.mu`，而 `rf.mu` 可能被长时间持有，增加延迟

3. **RPC 在锁外执行**：`rf.mu.Unlock()` 后才发 RPC。这很关键——如果持有锁发 RPC，RPC 可能因为网络延迟需要很长时间，导致所有其他操作被阻塞。

**doReplicate**（raft.go:468-477）：

```go
func (rf *Raft) doReplicate(server int, args interface{}) {
    switch v := args.(type) {
    case AppendEntriesArgs:
        rf.doAppendEntries(server, &v)
    case InstallSnapshotArgs:
        rf.doInstallSnapshot(server, &v)
    default:
        panic("doReplicate() args type not match!")
    }
}
```

根据 `prepareReplicationArgs` 返回的类型分发到不同的处理函数。

---

#### Applier —— 应用协程

```go
func (rf *Raft) applier() {
    for !rf.killed() {
        rf.mu.Lock()
        // 等待有新的已提交日志
        for rf.lastApplied >= rf.commitIndex {
            rf.applierCond.Wait()
        }

        commitIndex := rf.commitIndex
        lastApplied := rf.lastApplied
        entries := make([]LogEntry, commitIndex - lastApplied)
        copy(entries, rf.log[rf.toRealIndex(lastApplied+1):rf.toRealIndex(commitIndex+1)])

        // 优先发送快照消息
        if rf.msg != nil {
            msg := rf.msg
            rf.msg = nil
            rf.mu.Unlock()
            rf.applyCh <- *msg
        } else {
            rf.mu.Unlock()
        }

        // 逐条发送已提交的日志
        for _, entry := range entries {
            rf.applyCh <- ApplyMsg{
                CommandValid: true,
                Command:      entry.Command,
                CommandTerm:  entry.Term,
                CommandIndex: entry.Index,
            }
        }

        // 更新 lastApplied
        rf.mu.Lock()
        if rf.lastApplied < commitIndex {
            rf.lastApplied = commitIndex
        }
        rf.mu.Unlock()
    }
}
```

**逐段分析**：

**1. 等待条件**：
```go
for rf.lastApplied >= rf.commitIndex {
    rf.applierCond.Wait()
}
```
没有新提交的日志时 sleep。被 `AppendEntries` handler 或 `doAppendEntries` 中的 `rf.applierCond.Signal()` 唤醒。

**2. 拷贝待 apply 的日志**：
```go
commitIndex := rf.commitIndex
lastApplied := rf.lastApplied
entries := make([]LogEntry, commitIndex - lastApplied)
copy(entries, rf.log[rf.toRealIndex(lastApplied+1):rf.toRealIndex(commitIndex+1)])
```

**为什么要拷贝？** 因为随后要释放 `rf.mu`，如果直接使用 `rf.log` 的 slice，在释放锁后 `rf.log` 可能被修改（其他 goroutine 修改日志，比如 AppendEntries）。

**为什么用本地变量 `commitIndex` 和 `lastApplied`？** 释放锁后 `rf.commitIndex` 可能被更新。用本地快照保证本次 apply 的范围不会被意外改变。

**3. 优先发送快照**：
```go
if rf.msg != nil {
    msg := rf.msg
    rf.msg = nil
    rf.mu.Unlock()
    rf.applyCh <- *msg
}
```
`rf.msg` 是 InstallSnapshot handler 设置的。快照消息优先级更高，因为它可能影响后续日志的 apply。

**4. 更新 lastApplied**：
```go
rf.mu.Lock()
if rf.lastApplied < commitIndex {
    rf.lastApplied = commitIndex
}
rf.mu.Unlock()
```
使用 `rf.lastApplied < commitIndex`（严格小于），而不是 `rf.lastApplied = commitIndex`。因为 InstallSnapshot 可能已经把 `lastApplied` 跳到一个很大的值了，重新赋值为 `commitIndex` 会导致回退。

---

#### InstallSnapshot —— 快照安装

**Follower 端处理**（raft.go:639-682）：

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !rf.checkRequestTerm(args, reply) {
        return
    }

    // 快照比我已提交的还旧，忽略
    if args.LastIncludedIndex <= rf.commitIndex {
        return
    }

    rf.lastHeartBeat = time.Now()
    rf.commitIndex = args.LastIncludedIndex
    rf.lastApplied = args.LastIncludedIndex

    lastLogIndex := rf.log[len(rf.log)-1].Index

    if args.LastIncludedIndex >= lastLogIndex {
        // 快照覆盖了全部日志 → 全部替换
        rf.log = []LogEntry{{
            Index:   args.LastIncludedIndex,
            Term:    args.LastIncludedTerm,
            Command: nil,
        }}
    } else {
        // 快照只覆盖部分 → 截断前面
        realIdx := rf.toRealIndex(args.LastIncludedIndex)
        if realIdx >= 0 && rf.log[realIdx].Term == args.LastIncludedTerm {
            rf.log = append([]LogEntry{}, rf.log[realIdx:]...)
            rf.log[0].Command = nil
        } else {
            // Term 也不匹配，无法信任交叉部分，全部替换
            rf.log = []LogEntry{{
                Index:   args.LastIncludedIndex,
                Term:    args.LastIncludedTerm,
                Command: nil,
            }}
        }
    }
    rf.persister.Save(rf.encodeState(), args.Data)

    // 设置待发送的快照消息
    rf.msg = &ApplyMsg{
        SnapshotValid: true,
        Snapshot:      args.Data,
        SnapshotTerm:  args.LastIncludedTerm,
        SnapshotIndex: args.LastIncludedIndex,
    }
}
```

**三种 log 替换策略**：

1. **快照覆盖所有日志**（`LastIncludedIndex >= lastLogIndex`）：抛弃全部旧日志，只保留一个快照占位符。
2. **快照只覆盖部分 + 交叉点 Term 匹配**：截断交叉点之前的日志，保留交叉点及之后的。因为 Term 匹配说明这部分日志（>=LastIncludedIndex的部分）和快照一致。
3. **快照只覆盖部分 + 交叉点 Term 不匹配**：无法信任任何旧日志，全部替换。安全但保守。

**Leader 端处理回复**（raft.go:613-632）：

```go
func (rf *Raft) doInstallSnapshot(server int, args *InstallSnapshotArgs) {
    reply := &InstallSnapshotReply{}
    ok := rf.sendInstallSnapshot(server, args, reply)
    if !ok {
        return
    }
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if !rf.checkResponseTerm(args, reply, false) {
        return
    }

    // 检查快照是否过时
    if args.LastIncludedIndex != rf.log[0].Index {
        return  // 本地已经做了更新的快照，这次安装请求过期
    }

    rf.nextIndex[server] = args.LastIncludedIndex + 1
    rf.matchIndex[server] = args.LastIncludedIndex
    rf.persister.Save(rf.encodeState(), args.Data)
}
```

**关键检查**：`args.LastIncludedIndex != rf.log[0].Index`
- 在 RPC 往返期间，Leader 可能已经做了新的快照，`log[0].Index` 变大了
- 如果快照已经过时，`nextIndex` 和 `matchIndex` 的更新就不可靠，直接忽略

---

#### Ticker —— 定时器

```go
func (rf *Raft) ticker() {
    for !rf.killed() {
        rf.mu.Lock()
        if rf.stat == Leader {
            // Leader: 定期发心跳
            for i := range rf.peers {
                if i != rf.me {
                    args := rf.prepareReplicationArgs(i)
                    go rf.doReplicate(i, args)
                }
            }
        } else if rf.isElectionTimeout() {
            // Follower/Candidate: 超时则发起选举
            rf.launchElection()
        }
        rf.mu.Unlock()
        time.Sleep(HeartBeatGap * time.Millisecond)  // 125ms
    }
}
```

**工作循环**：
- 每 125ms 执行一次
- Leader → 发心跳（AppendEntries）
- 非 Leader → 检查是否选举超时 → 超时就发起选举

**为什么用 sleep 而不是 timer？**
- 更简单。Timer 需要处理重置、取消等复杂情况。
- Sleep 在 goroutine 中已经足够。125ms 的精度对于 300-500ms 的超时窗口来说足够。

**isElectionTimeout**（raft.go:721-725）：

```go
func (rf *Raft) isElectionTimeout() bool {
    timeoutRange := ElectionTimeoutMax - ElectionTimeoutMin
    timeout := ElectionTimeoutMin + rand.Intn(timeoutRange)
    return time.Now().After(rf.lastHeartBeat.Add(time.Duration(timeout) * time.Millisecond))
}
```

**随机超时**：300 + rand(0..200) = 300-499ms。随机化防止多个 Follower 同时超时、同时竞选，导致选票分散。

---

#### Make —— 初始化

```go
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    rf.applyCh = applyCh
    rf.lastHeartBeat = time.Now()
    rf.stat = Follower        // 初始为 Follower
    rf.voteFor = -1           // 还没投票
    rf.log = make([]LogEntry, 0)
    rf.log = append(rf.log, LogEntry{0, 0, nil})  // dummy entry（index 0）

    rf.commitIndex = 0
    rf.lastApplied = 0

    // 初始化条件变量
    rf.applierCond = sync.NewCond(&rf.mu)             // 使用 rf.mu 作为锁
    rf.replicatorCond = make([]*sync.Cond, len(peers))

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    for i := range peers {
        rf.nextIndex[i] = 1    // 初始为 1（dummy 之后的第一条）
        if i != rf.me {
            // ⚠️ 每个 replicator 有独立的锁！
            rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
            go rf.replicator(i)
        }
    }

    rf.msg = nil

    // 从持久化存储恢复状态
    rf.readPersist(persister.ReadRaftState())

    // 启动后台协程
    go rf.ticker()
    go rf.applier()

    return rf
}
```

**初始化要点**：

1. **Dummy Entry**：`log[0] = {0, 0, nil}`。它方便了索引计算：第一个真正的日志条目 index=1 对应数组下标 `toRealIndex(1) = 1`。

2. **双锁设计**：
   - `applierCond` 使用 `&rf.mu` 作为锁——因为只有一个 applier，且 applier 需要和 Raft 主逻辑共享状态
   - 每个 `replicatorCond[i]` 使用独立的 `&sync.Mutex{}`——因为有 N 个 replicator，且需要独立唤醒

3. **`nextIndex` 初始为 1**：乐观假设日志是同步的。第一次 AppendEntries 如果失败，通过快速回溯调整。

4. **崩溃恢复**：`readPersist` 在 `ticker` 和 `applier` 启动前调用，确保从持久化状态恢复后再开始工作。

---

#### RaftRPC 接口

```go
type RaftRPC interface {
    GetTerm() int
    SetTerm(int)
}
```

所有 RPC 的 Args 和 Reply 都实现此接口（raft.go:825-871）。这样 `checkRequestTerm` 和 `checkResponseTerm` 可以用多态的方式处理不同类型。

**checkRequestTerm**（raft.go:795-805）：

```go
// Reply false if term < currentTerm (§5.1)
// If RPC request contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkRequestTerm(args, reply RaftRPC) bool {
    term := args.GetTerm()
    defer reply.SetTerm(rf.currentTerm)  // 在返回前填写 reply 的 term
    if term < rf.currentTerm {
        return false  // 请求者 term 过期
    }
    if term > rf.currentTerm {
        rf.resetNewTermState(term)  // 我过期了，更新
    }
    return true
}
```

**关键细节**：`defer reply.SetTerm(rf.currentTerm)`——无论是否接受请求，都把当前 term 写入 reply，让对方知道我（可能）在更高的 term。

**checkResponseTerm**（raft.go:809-818）：

```go
// If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower (§5.1)
func (rf *Raft) checkResponseTerm(args, reply RaftRPC, isElection bool) bool {
    argsTerm := args.GetTerm()
    replyTerm := reply.GetTerm()
    if replyTerm > argsTerm {
        rf.resetNewTermState(replyTerm)
        rf.lastHeartBeat = time.Now()    // 给新 term 一个完整的超时窗口
        return false
    }
    return isElection || (rf.stat == Leader)
}
```

**两种模式**：
- `isElection=true`（选举）：只要回复的 term 不比我大，就继续。Candidate 总是关心回复。
- `isElection=false`（日志复制）：还需要我是 Leader 才关心回复。如果不是 Leader 了，日志复制的回复没有意义。

**为什么 `checkResponseTerm` 要比较 replyTerm 和 argsTerm，而不是和 rf.currentTerm？**

因为发送 RPC 和收到回复之间，`rf.currentTerm` 可能已经变了（收到更高 term 的 RPC）。如果直接比较 `replyTerm` 和 `rf.currentTerm`，可能误判。

标准化做法是：保存发送时的 term（`args.Term`），比较回复时的 term 和发送时的 term。

**resetNewTermState**（raft.go:784-790）：

```go
// Warning: this function is not thread-safe
func (rf *Raft) resetNewTermState(targetTerm int) {
    if rf.currentTerm < targetTerm {
        rf.voteFor = -1       // 新 term，清空投票记录
    }
    rf.currentTerm = targetTerm
    rf.stat = Follower        // 降级为 Follower
}
```

---

## Part B: KV Raft 容错键值服务 (src/kvraft)

### common.go

```go
package kvraft

const (
    OK             = "OK"
    ErrNoKey       = "ErrNoKey"
    ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type PutAppendArgs struct {
    Key      string
    Value    string
    OpStr    string  // "Put" 或 "Append"
    ClientId int64   // 客户端唯一标识（幂等去重用）
    Version  int     // 请求版本号（幂等去重用）
}

type PutAppendReply struct {
    Err Err
}

type GetArgs struct {
    Key      string
    ClientId int64   // 客户端唯一标识（幂等去重用）
    Version  int     // 请求版本号（幂等去重用）
}

type GetReply struct {
    Err   Err
    Value string
}
```

**错误类型**：
- `OK`：成功
- `ErrNoKey`：key 不存在
- `ErrWrongLeader`：当前 server 不是 Leader

注意 `Get` 也有 `ClientId` 和 `Version`——即使读操作也需要幂等去重。因为读操作也通过 Raft 日志提交，重复读请求不应该产生多条日志。

---

### server.go

#### 数据结构

```go
const WaitTimeout = 500  // 等待 Raft commit 的超时时间（ms）

type Op struct {
    Key      string
    Value    string
    OpStr    string   // "Get", "Put", "Append"
    ClientId int64
    Version  int
}

type result struct {
    term  int
    index int
    value string
    err   Err
}

type Content struct {
    Version   int
    LastValue string
    Err       Err
}

type KVServer struct {
    mu      sync.Mutex
    me      int
    rf      *raft.Raft            // Raft 共识层
    applyCh chan raft.ApplyMsg    // 接收 Raft commit 消息
    dead    int32

    maxRaftState int              // 触发 snapshot 的 Raft 状态大小阈值
    persister    *raft.Persister

    store     map[string]string          // 真正的键值存储
    waitChans map[int64]chan result      // term+index → 等待结果的 channel
    cache     map[int64]Content          // clientId → 最近一次成功请求结果（幂等缓存）
}
```

**核心数据流**：
1. 客户端 RPC → `Get`/`PutAppend` handler
2. Handler 构建 `Op` → `sendRaft(op, ch)` → `rf.Start(op)`
3. Raft commit → `applyCh` → `executor` 协程收到
4. Executor 执行操作 → 更新 `store` 和 `cache` → 通过 `waitChans` 通知等待者
5. Handler 收到通知 → 回复客户端

#### Get Handler

```go
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    ch := make(chan result)
    op := Op{
        Key:      args.Key,
        Value:    "",
        OpStr:    "Get",
        ClientId: args.ClientId,
        Version:  args.Version,
    }
    go kv.sendRaft(op, ch)
    res := <-ch     // 阻塞等待 Raft commit
    close(ch)
    reply.Value = res.value
    reply.Err = res.err
}
```

**流程**：
1. 创建一个无缓冲 channel
2. 构建 Op，通过 `sendRaft` 提交到 Raft（启动 goroutine 异步处理）
3. 阻塞等待 channel 返回结果
4. 收到结果后填充 reply，RPC 返回

**为什么用 goroutine 调用 `sendRaft`？**
因为 `sendRaft` 需要获取锁，而 `Get` handler 也需要在等待 channel 前持有锁（见下）。两者分离避免死锁。

#### PutAppend Handler

```go
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    ch := make(chan result)
    op := Op{
        Key:      args.Key,
        Value:    args.Value,
        OpStr:    args.OpStr,
        ClientId: args.ClientId,
        Version:  args.Version,
    }
    go kv.sendRaft(op, ch)
    res := <-ch     // 阻塞等待
    close(ch)
    reply.Err = res.err
}
```

与 Get 结构完全相同，唯一区别是 reply 中没有 Value 字段。

#### sendRaft —— 核心调度函数

```go
// 注意：map 不是线程安全的，调用者需持有锁
func (kv *KVServer) sendRaft(op Op, ch chan result) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    res := result{value: ""}

    // === 第一步：幂等检查 ===
    hit, con := kv.isCacheHit(op.ClientId, op.Version)
    if hit {
        res.value = con.LastValue
        res.err = con.Err
        ch <- res
        return  // 重复请求，不提交到 Raft
    }

    // === 第二步：提交到 Raft ===
    index, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        res.err = ErrWrongLeader
        ch <- res
        return
    }

    // === 第三步：注册等待 channel ===
    waitChan := kv.makeWaitChan(term, index)
    go kv.waitExecute(term, index, ch, waitChan)
}
```

**三步走**：
1. **幂等检查**：查 `cache`，如果这个 client 的这个 version 已经处理过，直接返回缓存结果
2. **提交 Raft**：`rf.Start(op)` 将 Op 作为日志提交。如果不是 Leader，返回错误
3. **等待 commit**：注册一个 channel，等待 Raft commit 后 executor 通知

#### isCacheHit —— 幂等检查

```go
func (kv *KVServer) isCacheHit(clientId int64, version int) (bool, Content) {
    c, ok := kv.cache[clientId]
    if ok && c.Version >= version {
        return true, c
    }
    return false, Content{}
}
```

**版本比较**：`c.Version >= version`。如果缓存中的版本号 >= 请求的版本号，说明该请求（或更新的请求）已经被处理过。使用 `>=` 而不是 `==` 是为了安全——即使客户端重复发送旧版本请求，也能被正确拦截。

#### makeWaitChan / deleteWaitChan

```go
func (kv *KVServer) makeWaitChan(term int, index int) chan result {
    waitChan := make(chan result, 1)    // buffered channel（容量 1）
    chanId := getChanId(term, index)
    kv.waitChans[chanId] = waitChan
    return waitChan
}

func (kv *KVServer) deleteWaitChan(term int, index int) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    id := getChanId(term, index)
    close(kv.waitChans[id])
    delete(kv.waitChans, id)
}

func getChanId(term int, index int) int64 {
    id := int64(term) << 32
    id += int64(index)
    return id
}
```

**Channel ID 编码**：`(term << 32) | index`。因为 (term, index) 唯一标识了一条 Raft 日志条目，用它可以精确匹配请求和结果。

**Buffered channel (容量 1)**：避免 executor 发送结果时阻塞。`deleteWaitChan` 中先 close 再 delete——close 不会导致 panic（从一个已 close 的 channel 读取会返回零值），delete 回收内存。

#### waitExecute —— 等待结果

```go
func (kv *KVServer) waitExecute(term int, index int, ch chan result, waitChan chan result) {
    select {
    case <-time.After(WaitTimeout * time.Millisecond):
        // 超时：可能是 Leader 崩溃了
        res := result{
            term:  term,
            index: index,
            value: "",
            err:   ErrWrongLeader,
        }
        ch <- res
    case res := <-waitChan:
        ch <- res
    }
    kv.deleteWaitChan(term, index)
}
```

**两条路径**：
1. **正常**：executor 通过 `waitChan` 发来结果
2. **超时**（500ms）：假设 Leader 崩溃了，返回 `ErrWrongLeader`。客户端会重试到新的 Leader。

**为什么不无限等待？** Raft 可能在 commit 前 crash，对应的日志条目永远不会被 commit。无限等待会导致 handler goroutine 泄漏。

#### executor —— 状态机执行协程

```go
func (kv *KVServer) executor() {
    for !kv.killed() {
        msg := <-kv.applyCh      // 从 Raft 收到消息
        kv.mu.Lock()

        if msg.CommandValid {
            // === 处理命令 ===
            op := msg.Command.(Op)   // 类型断言
            term := msg.CommandTerm
            index := msg.CommandIndex

            res := result{term: term, index: index, value: "", err: OK}

            // 幂等检查（再次！）
            hit, con := kv.isCacheHit(op.ClientId, op.Version)

            if !hit {
                // 首次执行：实际执行命令
                switch op.OpStr {
                case "Get":
                    if v, ok := kv.store[op.Key]; ok {
                        res.value = v
                    } else {
                        res.err = ErrNoKey
                    }
                case "Put":
                    kv.store[op.Key] = op.Value
                case "Append":
                    kv.store[op.Key] += op.Value
                default:
                    panic("unknown op type!")
                }
                // 写入缓存（幂等去重）
                kv.cache[op.ClientId] = Content{
                    Version:   op.Version,
                    LastValue: res.value,
                    Err:       res.err,
                }
            } else {
                // 重复请求：使用缓存结果
                res.value = con.LastValue
                res.err = con.Err
            }

            // 通知等待者
            if ch, ok := kv.waitChans[getChanId(term, index)]; ok {
                select {
                case ch <- res:
                    // 发送成功
                default:
                    panic("channel is full or closed")
                }
            }

            // 检查是否需要 snapshot
            if kv.maxRaftState != -1 && kv.persister.RaftStateSize() > kv.maxRaftState {
                kv.rf.Snapshot(index, kv.encode())
            }

        } else if msg.SnapshotValid {
            // === 处理快照 ===
            kv.decode(msg.Snapshot)   // 用快照替换状态机
        }

        kv.mu.Unlock()
    }
}
```

**为什么 executor 中还要做一次幂等检查？**

因为在 `sendRaft` 和实际 commit 之间有时间差。考虑场景：
1. 客户端发送请求 A (version=5)，`sendRaft` 中 cache miss，提交到 Raft
2. Raft commit 前，客户端重试请求 A (version=5)
3. 第二次 `sendRaft` 中 cache miss（第一次还没执行完），再次提交到 Raft
4. Raft 提交第一条 → executor 执行，写入 cache
5. Raft 提交第二条 → executor 检查 cache：hit！不再执行，直接使用缓存结果

**通知机制**：
`waitChans[getChanId(term, index)]` —— 如果有等待者，发结果给它。如果没有等待者（比如这个 request 是另一个 server 作为 Leader 时提交的，但当前节点变成了 Follower），就什么都不做。

**快照触发**：
```go
if kv.maxRaftState != -1 && kv.persister.RaftStateSize() > kv.maxRaftState {
    kv.rf.Snapshot(index, kv.encode())
}
```
当 Raft 的状态大小超过阈值（`maxRaftState`），触发快照。`kv.encode()` 序列化当前状态机的 `store` 和 `cache`。

#### encode / decode —— 持久化状态机

```go
func (kv *KVServer) encode() []byte {
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(kv.cache)
    e.Encode(kv.store)
    return w.Bytes()
}

func (kv *KVServer) decode(data []byte) {
    if data == nil || len(data) < 1 {
        return
    }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var cache map[int64]Content
    var store map[string]string
    if d.Decode(&cache) != nil ||
        d.Decode(&store) != nil {
        fmt.Println("[error] can't read persist state")
    } else {
        kv.cache = cache
        kv.store = store
    }
}
```

序列化和反序列化状态机状态。**同时保存 cache 和 store**——cache 包含幂等去重信息，在崩溃恢复时必须保留，否则已执行的请求可能被重复执行。

#### StartKVServer —— 初始化

```go
func StartKVServer(servers []*labrpc.ClientEnd, me int,
    persister *raft.Persister, maxRaftState int) *KVServer {

    labgob.Register(Op{})  // 注册 Op 类型，让 gob 知道怎么编解码

    kv := new(KVServer)
    kv.me = me
    kv.applyCh = make(chan raft.ApplyMsg)
    kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.persister = persister
    kv.dead = int32(0)
    kv.maxRaftState = maxRaftState

    kv.store = make(map[string]string)
    kv.waitChans = make(map[int64]chan result)
    kv.cache = make(map[int64]Content)

    // 恢复持久化状态
    kv.decode(kv.persister.ReadSnapshot())

    go kv.executor()
    return kv
}
```

**`labgob.Register(Op{})`**：Go 的 RPC 框架需要知道 Op 类型才能序列化。`raft.Start` 的 `command` 参数是 `interface{}`，在网络传输和持久化时需要反射创建具体类型的实例。

---

### client.go

#### Clerk 结构体

```go
type Clerk struct {
    servers  []*labrpc.ClientEnd
    id       int64    // 客户端唯一标识（随机生成）
    version  int      // 请求计数器
    leaderId int      // 缓存的 Leader ID
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
    ck.id = nrand()     // 用加密安全的随机数生成器
    ck.version = 0
    ck.leaderId = 0
    return ck
}
```

**`nrand()`**：使用 `crypto/rand`（加密安全随机数），生成 62-bit 的唯一 client ID。使用加密随机数避免了多个 client 在同时启动时发生 ID 碰撞。

#### Get 请求

```go
func (ck *Clerk) Get(key string) string {
    ck.version++         // 每次请求递增版本号
    args := GetArgs{
        Key:      key,
        ClientId: ck.id,
        Version:  ck.version,
    }
    leaderId := ck.leaderId
    for {
        // 轮询所有 server，从缓存的 leader 开始
        for i := 0; i < len(ck.servers); i++ {
            peer := (leaderId + i) % len(ck.servers)
            reply := GetReply{}
            ok := ck.servers[peer].Call("KVServer.Get", &args, &reply)
            if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
                ck.leaderId = peer  // 缓存成功的 server
                return reply.Value
            }
        }
        time.Sleep(RPCGap * time.Millisecond)  // 100ms 后重试
    }
}
```

**重试策略**：
1. 从 `leaderId` 开始轮询（上次请求成功的 server）
2. 每个 server 逐一尝试
3. 只有 `Err == OK` 或 `Err == ErrNoKey` 才算成功——这两个错误码表示请求已经被正确处理
4. `ErrWrongLeader` 或其他错误 → 尝试下一个 server
5. 全部失败 → sleep 100ms 后重试
6. 无限重试直到成功

**为什么可以无限重试？** 因为幂等性保证了重复请求不会产生副作用。

#### PutAppend 请求

```go
func (ck *Clerk) PutAppend(key string, value string, op string) {
    ck.version++
    args := PutAppendArgs{
        Key:      key,
        Value:    value,
        OpStr:    op,
        ClientId: ck.id,
        Version:  ck.version,
    }
    leaderId := ck.leaderId
    for {
        for i := 0; i < len(ck.servers); i++ {
            peer := (leaderId + i) % len(ck.servers)
            reply := GetReply{}
            ok := ck.servers[peer].Call("KVServer.PutAppend", &args, &reply)
            if ok && reply.Err == OK {
                ck.leaderId = peer
                return
            }
        }
        time.Sleep(RPCGap * time.Millisecond)
    }
}

func (ck *Clerk) Put(key string, value string)  { ck.PutAppend(key, value, "Put") }
func (ck *Clerk) Append(key string, value string) { ck.PutAppend(key, value, "Append") }
```

与 Get 基本相同，但只接受 `Err == OK` 作为成功（`ErrNoKey` 只对 Get 有意义）。

---

## 总结

整个实现总共约 1200 行代码，但涵盖了分布式共识算法的核心：

| 模块 | 代码量 | 核心内容 |
|---|---|---|
| Raft (raft.go) | ~870 行 | Leader 选举、日志复制、快照、持久化、并发控制 |
| KV Server (server.go) | ~320 行 | 线性一致读写、幂等去重、快照触发、结果通知 |
| KV Client (client.go) | ~110 行 | Leader 发现、重试策略 |
| KV Common (common.go) | ~38 行 | RPC 参数/返回值定义 |

**写这 1200 行的过程远比读它困难**——每一个 if 条件、每一个 defer、每一个锁的获取和释放，背后都对应着一个分布式系统中的微妙场景。希望这份指南能帮你更快地理解这些细节。

---

> 建议阅读顺序：Raft 数据结构 → Make 初始化 → Ticker/选举 → AppendEntries/日志复制 → Applier → InstallSnapshot/快照 → KV Server → KV Client
