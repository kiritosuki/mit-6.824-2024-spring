package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type content struct {
	version   int
	lastValue string
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	store map[string]string // key - value
	cache map[int64]content // id - version & lastValue
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.store[args.Key]
	if !ok {
		reply.Value = ""
		return
	}
	reply.Value = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	c, ok := kv.cache[args.Id]
	reply.Value = ""
	if ok && c.version >= args.Version {
		// old RPC
		return
	}
	kv.cache[args.Id] = content{
		version: args.Version,
	}
	kv.store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	c, ok := kv.cache[args.Id]
	if ok && c.version >= args.Version {
		// old RPC
		reply.Value = c.lastValue
		return
	}
	oldValue := ""
	if v, exist := kv.store[args.Key]; exist {
		oldValue = v
	}
	kv.cache[args.Id] = content{
		version:   args.Version,
		lastValue: oldValue,
	}
	kv.store[args.Key] = oldValue + args.Value
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.cache = make(map[int64]content)
	return kv
}
