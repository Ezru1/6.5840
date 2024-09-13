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

// type KV struct{
// 	Key string
// 	Value string
// }

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVMAP map[string]string
	IdMap map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.KVMAP[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.IdMap[args.Id]
	if ok {
		reply.Value = v
		return
	}
	// fmt.Println(len(kv.IdMap))
	kv.KVMAP[args.Key] = args.Value
	kv.IdMap[args.Id] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	v, ok := kv.IdMap[args.Id]
	if ok {
		reply.Value = v
		return
	}
	value := kv.KVMAP[args.Key]
	kv.KVMAP[args.Key] += args.Value
	kv.IdMap[args.Id] = value
	reply.Value = value
}

func (kv *KVServer) Free(args *FreeArgs, reply *FreeReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.IdMap, args.Id)
	// fmt.Println(len(kv.IdMap))
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.KVMAP = make(map[string]string)
	kv.IdMap = make(map[int64]string)

	return kv
}
