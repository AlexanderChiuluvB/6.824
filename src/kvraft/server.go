package raftkv

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"time"
)

const Debug = 0
const WaitPeriod = time.Duration(1000) * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string
	Key    string
	Value  string
	Clerk  int64 //哪个clerk发出
	Index  int   //这个clerk的第几条命令
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	msgCh     map[int]chan int  //消息通知的管道 key:index value: 用于通信的管道
	kvDB      map[string]string //KV数据库
	clerkLog  map[int64]int     //记录客户及其对应的指令
	persister *raft.Persister
}

/**
Server处理GET操作
*/

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//1.把RPC附带的参数如客户编号,客户指令编号,操作等填入op,作为一条新指令
	op := Op{"Get", args.Key, "", args.Cid, args.CmdIndex}
	reply.Err = ErrNoKey
	reply.WrongLeader = true

	//2.把这条新指令传递给Server相关联的rf start函数
	index, term, isLeader := kv.rf.Start(op)
	//3.跟胡返回结果,判断本server对应的raft是否是leader
	if !isLeader {
		return
	}

	kv.mu.Lock()
	if ind, ok := kv.clerkLog[args.Cid]; ok && ind >= args.CmdIndex {
		//该指令已经执行
		//raft.InfoKV.Printf("KVServer:%2d | Cmd has been finished: Method:[%s] clerk:[%v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
		reply.Value = kv.kvDB[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
	raft.InfoKV.Printf("KVServer:%2d | leader msgIndex:%4d\n", kv.me, index)
	ch := make(chan int)
	//为client index创建相应的通道 等待管道信息
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	select {
	case <-time.After(WaitPeriod):
		raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Timeout!\n", kv.me, index, term)

	case msgTerm := <-ch:
		//只有当申请时候server的leader等于接收时候的leader才相等
		if msgTerm == term {
			kv.mu.Lock()
			if val, ok := kv.kvDB[args.Key]; ok {
				reply.Value = val
				reply.Err = OK
			}
			kv.mu.Unlock()
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Not leader any more!\n", kv.me, index, term)
		}
	}
	go func() { kv.closeCh(index) }()
	return
}

//Server处理PutAppend
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// Your code here.
	//1.把RPC附带的参数如客户编号,客户指令编号,操作等填入op,作为一条新指令
	op := Op{args.Op, args.Key, args.Value, args.Cid, args.CmdIndex}
	reply.Err = OK
	kv.mu.Lock()
	if ind, ok := kv.clerkLog[args.Cid]; ok && ind >= args.CmdIndex {
		//该指令已经执行
		kv.mu.Unlock()
		//raft.InfoKV.Printf("KVServer:%2d | Cmd has been finished: Method:[%s] clerk:[%v] index:[%4d]\n", kv.me, op.Method, op.Clerk, op.Index)
		reply.WrongLeader = false
		return
	}
	kv.mu.Unlock()

	//2.把这条新指令传递给Server相关联的rf start函数
	index, term, isLeader := kv.rf.Start(op)
	//3.根据返回结果,判断本server对应的raft是否是leader
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | leader msgIndex:%4d\n", kv.me, index)
	ch := make(chan int)
	//为client index创建相应的通道 等待管道信息
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	select {
	case <-time.After(WaitPeriod):
		raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Timeout!\n", kv.me, index, term)

	case msgTerm := <-ch:
		//只有当申请时候server的leader等于接收时候的leader才相等
		if msgTerm == term {
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} OK!\n", kv.me, index, term)
			reply.WrongLeader = false
		} else {
			raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Not leader any more!\n", kv.me, index, term)
		}
	}
	go func() { kv.closeCh(index) }()

	return
}

func (kv *KVServer) closeCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | KV server is died!\n", kv.me)
	kv.mu.Unlock()

}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	kv.msgCh = make(map[int]chan int) //消息通知的管道 key:index value: 用于通信的管道
	kv.kvDB = make(map[string]string) //KV数据库
	kv.clerkLog = make(map[int64]int) //记录客户及其对应的指令
	kv.persister = persister
	kv.loadSnapshot()
	go kv.receiveMessage()

	return kv
}

func (kv *KVServer) loadSnapshot() {

	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) == 0 {
		return
	}
	kv.decodedSnapshot(data)

}

func (kv *KVServer) decodedSnapshot(data []byte) {

	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var db map[string]string
	var cl map[int64]int

	if dec.Decode(&db) != nil || dec.Decode(&cl) != nil {

	} else {
		kv.kvDB = db
		kv.clerkLog = cl
	}

}

func (kv *KVServer) receiveMessage() {

	for msg := range kv.applyCh {

		kv.mu.Lock()
		index := msg.CommandIndex
		term := msg.CommitTerm
		op := msg.Command.(Op)

		if !msg.CommandValid {
			op := msg.Command.([]byte)
			kv.decodedSnapshot(op)
			kv.mu.Unlock()
			continue
		}

		//clerkLog[index]存储着index对应客户端的指令的index,如果操作的index小于等于
		//客户端对应的index,说明这个操作其实已经执行过了
		if ind, ok := kv.clerkLog[op.Clerk]; ok && ind >= op.Index {

		} else {
			kv.clerkLog[op.Clerk] = op.Index
			switch op.Method {
			case "Put":
				kv.kvDB[op.Key] = op.Value
			case "Append":
				if _, ok := kv.kvDB[op.Key]; ok {
					kv.kvDB[op.Key] = kv.kvDB[op.Key] + op.Value
				} else {
					kv.kvDB[op.Key] = op.Value
				}
			case "Get":
			}
		}

		if ch, ok := kv.msgCh[index]; ok {
			ch <- term
		}
		kv.checkState(index, term)
		kv.mu.Unlock()
	}
}


func (kv *KVServer) checkState(index int, term int) {

	if kv.maxraftstate == -1 {
		return
	}

	portion := 2/3

	if kv.persister.RaftStateSize() < kv.maxraftstate * portion{
		return
	}

	rawSnapshot := kv.encodeSnapshot()
	go func() {kv.rf.TakeSnapshot(rawSnapshot, index, term)}()

}

func (kv *KVServer) encodeSnapshot() []byte {
	//调用者默认拥有kv.mu
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.kvDB)
	enc.Encode(kv.clerkLog)
	data := w.Bytes()
	return data
}