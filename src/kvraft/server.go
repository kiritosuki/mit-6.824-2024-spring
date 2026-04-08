package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpStr    string
	ClientId int64
	Version  int
}

type result struct {
	term  int
	index int
	value string
	err   Err
}

type content struct {
	version   int
	lastValue string
	err       Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	store     map[string]string
	waitChans map[int64]chan result
	cache     map[int64]content
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ch := make(chan result)
	op := Op{
		Key:      args.Key,
		Value:    "",
		OpStr:    "Get",
		ClientId: args.ClientId,
		Version:  args.Version,
	}
	go kv.sendRaft(op, ch)
	// block here
	res := <-ch
	close(ch)
	reply.Value = res.value
	reply.Err = res.err
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ch := make(chan result)
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpStr:    args.OpStr,
		ClientId: args.ClientId,
		Version:  args.Version,
	}
	go kv.sendRaft(op, ch)
	// block here
	res := <-ch
	close(ch)
	reply.Err = res.err
}

// attend that map is not thread safe
func (kv *KVServer) sendRaft(op Op, ch chan result) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := result{
		value: "",
	}
	hit, con := kv.isCacheHit(op.ClientId, op.Version)
	if hit {
		res.value = con.lastValue
		res.err = con.err
		ch <- res
		return
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.err = ErrWrongLeader
		ch <- res
		return
	}
	waitChan := kv.makeWaitChan(term, index)
	go kv.waitExecute(term, index, ch, waitChan)
}

func (kv *KVServer) waitExecute(term int, index int, ch chan result, waitChan chan result) {
	stamp := time.Now()
	select {
	case res := <- waitChan:
		ch <- res
		case
	}
}

func (kv *KVServer) makeWaitChan(term int, index int) chan result {
	waitChan := make(chan result, 1)
	chanId := getChanId(term, index)
	kv.waitChans[chanId] = waitChan
	return waitChan
}

func (kv *KVServer) deleteWaitChan(term int, index int) {

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.dead = int32(0)
	kv.maxRaftState = maxRaftState
	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.waitChans = make(map[int64]chan result)
	kv.cache = make(map[int64]content)
	// background running goroutine
	go kv.executor()
	return kv
}

// receive log from applyCh and execute, send out the result
// attend that map is not thread safe
func (kv *KVServer) executor() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			term := msg.CommandTerm
			index := msg.CommandIndex
			res := result{
				term:  term,
				index: index,
				value: "",
				err:   OK,
			}
			hit, con := kv.isCacheHit(op.ClientId, op.Version)
			if !hit {
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
				kv.cache[op.ClientId] = content{
					version:   op.Version,
					lastValue: res.value,
					err:       res.err,
				}
			} else {
				res.value = con.lastValue
				res.err = con.err
			}
			// why not full: each RPC request use the only channel, ensured by term and index
			// why not closed: before close, goroutine try to get lock
			if ch, ok := kv.waitChans[getChanId(term, index)]; ok {
				select {
				case ch <- res:
				default:
					panic("channel is full or closed")
				}
			}
		}
		kv.mu.Unlock()
	}
}

func getChanId(term int, index int) int64 {
	id := int64(term) << 32
	id += int64(index)
	return id
}

func (kv *KVServer) isCacheHit(clientId int64, version int) (bool, content) {
	c, ok := kv.cache[clientId]
	if ok && c.version >= version {
		return true, c
	}
	return false, content{}
}
