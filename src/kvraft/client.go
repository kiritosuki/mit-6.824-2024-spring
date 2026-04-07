package kvraft

import (
	"time"

	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"

const (
	RPCGap = 10
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id       int64
	version  int
	leaderId int
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
	// You'll have to add code here.
	ck.id = nrand()
	ck.version = 0
	ck.leaderId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.version++
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
		Version:  ck.version,
	}
	reply := GetReply{}
	ok := false
	for {
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			default:
				panic("unknown Err type!")
			}
		}
		time.Sleep(RPCGap * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.version++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		OpStr:    op,
		ClientId: ck.id,
		Version:  ck.version,
	}
	reply := PutAppendReply{}
	ok := false
	for {
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			default:
				panic("unknown or invalid Err type!")
			}
		}
		time.Sleep(RPCGap * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
