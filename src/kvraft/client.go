package raftkv

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../raft"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int   //记录最新的leader
	Cid      int64 //每个clerk都有独一无二的编号
	CmdIndex int   //clerk给每个RPC调用编号
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
	ck.leader = 0
	ck.Cid = nrand()
	ck.CmdIndex = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

/*
一个无限循环直到指令发送成功,循环内部是一个RPC调用,如果成功就立刻返回
如果由于超时或者所联系的server对应的raft不是Leader,则更换一个server发起指令
当client联系到leader的时候会记录leader的编号,那么下一次执行新的命令的时候,会直接
联系leader,而不是从头开始遍历寻找leader
*/
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	leader := ck.leader
	ck.CmdIndex++
	args := GetArgs{key, ck.Cid, ck.CmdIndex}
	raft.InfoKV.Printf("Client:%20v cmdIndex:%4d Begin! Get:[%v] from server %3d \n",
		ck.Cid, ck.CmdIndex, key, leader)

	for {

		reply := GetReply{}
		//RPC调用
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.leader = leader
			if reply.Value == ErrNoKey {
				raft.InfoKV.Printf("Client %20v cmdIndex:%4d Get failed! No such key!",
					ck.Cid, ck.CmdIndex)
				return ""
			}
			raft.InfoKV.Printf("Client %20v cmdIndex:%4d Successful! Get:[%v] "+
				"from server:%3d value:[%v]",
				ck.Cid, ck.CmdIndex, key, leader, reply.Value)
			raft.InfoKV.Printf("")
			return reply.Value
		}
		//如果对面的server不是leader,
		leader = (leader + 1) % len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// You will have to modify this function.
	leader := ck.leader
	ck.CmdIndex++
	args := PutAppendArgs{key, value, op, ck.Cid, ck.CmdIndex}
	raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Begin! %6s key:[%s] value:[%s] " +
		"to server:%3d\n", ck.Cid, ck.CmdIndex, op, key, value, leader)

	for {

		reply := PutAppendReply{}
		//RPC调用
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.leader = leader
			return

		}
		//如果对面的server不是leader,
		leader = (leader + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
