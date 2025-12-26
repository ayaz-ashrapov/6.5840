package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...any) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	m map[string]Pair
}

type Pair struct {
	Value   string
	Version uint64
}

func MakeKVServer() *KVServer {
	kv := &KVServer{m: make(map[string]Pair)}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	key := args.Key
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.m[key]; ok {
		reply.Value = v.Value
		reply.Version = rpc.Tversion(v.Version)
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	version := args.Version
	if pair, ok := kv.m[key]; ok {
		if pair.Version == uint64(version) {
			newVersion := pair.Version + 1
			pair.Version = newVersion
			pair.Value = args.Value
			kv.m[key] = pair
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else if version == 0 {
		newPair := Pair{}
		newPair.Value = args.Value
		newPair.Version = 1
		kv.m[key] = newPair
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
