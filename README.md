# MIT 6.824 / 6.5840 Distributed Systems Labs

这是我完成的 MIT 6.824 / 6.5840 Spring 2024 分布式系统课程实验项目。

目前主要完成了以下部分：

- Lab 2: KV Server
- Lab 3: Raft
- Lab 4: Fault-tolerant Key/Value Service with Raft

课程模板中还保留了其他实验目录，但本仓库重点实现和测试的是 `kvsrv`、`raft`、`kvraft` 这几部分。

## 实现内容

### Lab 2: KV Server

实现了一个单机键值服务，支持：

- `Get`
- `Put`
- `Append`
- 客户端请求去重
- 基于版本号的重复 RPC 处理

对应目录：

```text
src/kvsrv
```

### Lab 3: Raft

实现了 Raft 共识算法的核心机制，包括：

- Leader election
- Log replication
- Persistence
- Snapshot / log compaction
- Apply committed log entries to service

对应目录：

```text
src/raft
```

### Lab 4: KV Raft

基于 Lab 3 的 Raft 实现，构建了一个具备容错能力的分布式键值存储服务，支持：

- 多副本状态机复制
- `Get` / `Put` / `Append`
- Leader 转发与错误返回
- 客户端请求去重
- Snapshot 限制 Raft 日志增长
- 在网络不稳定、节点重启等场景下保持服务一致性

对应目录：

```text
src/kvraft
```

## 文档

为了更好地理解这个项目，我编写了两份详细的文档：

- **[INTERVIEW_QA.md](./INTERVIEW_QA.md)** —— 面试问答文档，以面试官和候选人对答的形式，深入讲解 Raft 共识算法和 KV Raft 的实现细节。涵盖：分布式共识原理、Leader 选举、日志复制、Commit 机制、持久化、快照、线性一致性、幂等性、代码亮点与难点等。
- **[BABY_GUIDE.md](./BABY_GUIDE.md)** —— 代码逐行讲解指南，对 `src/raft` 和 `src/kvraft` 中的每一段实现代码进行详细解释，包括数据结构设计、函数逻辑、并发控制、边界情况处理等。

## 项目结构

```text
.
├── Makefile
└── src
    ├── go.mod
    ├── kvsrv        # Lab 2: single-node key/value server
    ├── raft         # Lab 3: Raft implementation
    ├── kvraft       # Lab 4: fault-tolerant KV service based on Raft
    ├── labgob       # course support package
    ├── labrpc       # course RPC simulation package
    └── ...
```

## 测试情况

不保证绝对 bug free，但 `kvsrv`、`raft`、`kvraft` 均经过 500+ 次重复测试，测试过程中无 error。

可在 `src` 目录下运行对应测试：

```bash
cd src
go test ./kvsrv
go test ./raft
go test ./kvraft
```

也可以针对单个 lab 目录进行多轮测试，例如：

```bash
cd src/raft
go test
```

## 环境

- Go 1.15
- MIT 6.5840 / 6.824 Spring 2024 lab framework

## 说明

这个项目主要用于记录我完成 MIT 6.824 / 6.5840 分布式系统实验的过程。实现以通过课程测试为目标，重点覆盖 Raft 共识、复制状态机和基于 Raft 的容错键值服务。

## License

This project is licensed under the [MIT License](./LICENSE).
