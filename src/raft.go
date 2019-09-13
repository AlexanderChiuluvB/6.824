package src

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)
import "./labrpc"
import "./labgob"

// import "bytes"
// import "encoding/gob"


const (

	Leader = "Leader"
	Candidate =  "Candidate"
	Follower  = "Follower"
	heartBeat = time.Duration(100)
	RPC_CALL_TIMEOUT = time.Duration(500) * time.Millisecond
	votedNull = -1 //本轮没有投票给任何人

)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {

	//Index       int
	CommandValid bool //信息是否正确交付
	Command     interface{}
	CommandIndex int

	//CommitTerm int //交付信息时候的term
	//Role string //交付信息时raft的信息
	//UseSnapshot bool   // ignore for lab2; only used in lab3
	//Snapshot    []byte // ignore for lab2; only used in lab3
}



type Entries struct {
	CurrentTerm int
	Command 	interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	printLog 	bool //是否打日志

	currentTerm int //server存储的最新任期
	votedFor	int //代表所投的server,如果为-1,说明还没有投票
	votedThisTerm int //判断是在哪一轮承认的leader,防止一个节点多次投票
	log []Entries //日志用于保存执行的命令


	//以下是所有机器的可变状态
	commitIndex int //最后一个提交日志的index
	lastApplied int //最后一个应用到状态机的日志的index

	//以下是leader的可变状态

	nextIndex []int    //发给每个follower的下一个日志的index,初始化为leader最后一条日志+1 key:follower ID value:log index
	matchIndex []int  //对于每一个follower,leader与其对应的最后一条与该follower同步的日志的index,初始化为0,是follower最后一条日志

	appendCh chan *AppendEntriesArgs  //用于follower判断在election timeout时间有没有发送心跳信号
	voteCH 	chan *RequestVoteArgs     //投票后重启定时器

	applyCh chan ApplyMsg //实验中执行命令相当于给channel发送信息
	role string

	electionTimeOut *time.Timer
	heartBeatTimeout int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf(rf.printLog, "info", "me:%2d term:%3d | Role:%10s Persist data! VotedFor:%3d len(Logs):%3d\n", rf.me, rf.currentTerm, rf.role, rf.votedFor, len(rf.log))
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var logs []Entries

	if dec.Decode(&term) != nil || dec.Decode(&votedFor) !=nil || dec.Decode(&logs) != nil{
		//(rf.printLog, "info", "me:%2d term:%3d | Failed to read persist data!\n")
	}else{
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		DPrintf(rf.printLog, "info", "me:%2d term:%3d | Read persist data successful! VotedFor:%3d len(Logs):%3d\n", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	}

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// candidate用于发起选票
// RPC 的参数与方法首字母一定要大写
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term int //term of candidate
	CandidateId int // ID of candidate

	//下面这两个参数用来合起来比较candidate和
	LastLogIndex int //候选者最后一条日志记录的索引
	LastLogTerm int //候选者最后一条日志记录的索引的任期

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
// 这里的意思是candidate向接受者发起投票请求后从接受者得到的回复
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //返回接收者的currentTerm 一般是针对过期的leader

	/**
	如果 candidate.term < 接收者.term return false
	如果 voteFor == (null || candidateID)
	并且 candidate日志 == 本地日志,则投票给候选者
	投票之后,投票人的votedFor 会改变
	 */
	VoteGranted bool //如果候选者得到了选票则为true
}


//站在leader的角度
// 被leader用来复制日志
// 同时也可以作为心跳
type AppendEntriesArgs struct {

	Term int //leader任期
	LeaderId int //用来个follower重定向leader的ID
	PrevLogIndex int //leader上一次发送的日志的index
	PrevLogTerm int //term of prevLogIndex
	Entries []Entries
	LeaderCommit int //leader的提交的index
}


//站在接收者的角度
//把信息返回给leader
type AppendEntriesReply struct {

	Term int  // 接收到信息的follower的任期,方便过期leader更新信息
	Success bool //如果follower包含索引为prevLogIndex以及任期为preLogItem的日志

	// follower节点与第一个args.Term不相同的日志下标
	// 如果follower和leader隔了几个term
	// 只要找到 leader 中等于conflictTerm 最后一个日志
	// 就能一次性添加所有follower缺失的日志
	ConflictIndex int //冲突日志处的下标
	ConflictTerm int  //冲突日志处
}


func (rf *Raft) leader() {

	rf.mu.Lock()
	curLogLen := len(rf.log) -1
	// 保存执行leader程序时候的term
	// 后续rpc调用都用这个term
	// 可以快速检查到leader转为follower的情况
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	/**
	初始化rf的nextIndex 与 matchIndex
	 */
	for followerId, _ := range rf.peers {

		if followerId == rf.me {
			continue
		}
		rf.nextIndex[followerId] = curLogLen + 1
		rf.matchIndex[followerId] = 0
	}

	for {
		commitFlag := true
		commitNum :=1
		commitL := new(sync.Mutex)
		//这是信号量,用来通知前一半的commit节点,如果这个日志commit了,就可以修改对应的
		//leader.nextIndex[server]
		commitCond := sync.NewCond(commitL)

		for followerID, _ := range rf.peers {

			if followerID == rf.me {
				continue
			}

			rf.mu.Lock()
			appendArgs := &AppendEntriesArgs{
				curTerm,
				rf.me,
				rf.nextIndex[followerID] - 1,
				rf.log[rf.nextIndex[followerID]-1].CurrentTerm,
				rf.log[rf.nextIndex[followerID]:],
				rf.commitIndex}
			rf.mu.Unlock()

			//发送心跳信息
			go func(server int) {
				reply := &AppendEntriesReply{}
				//DPrintf("info", "me:%2d term:%3d | leader send message to %3d\n", rf.me, curTerm, server)
				if ok := rf.peers[server].Call("Raft.AppendEntries", appendArgs, reply); ok {

					appendEntriesLen := len(appendArgs.Entries)

					rf.mu.Lock()
					defer rf.mu.Unlock()


					if rf.currentTerm != curTerm || appendEntriesLen ==0 {
						// 如果发送信息时候的term不等于当前server的term 说明这是过期信息,不要了
						// appendEntriesLen = 0 表示这是心跳信息
						return
					}

					if reply.Success {

						curCommitLen := appendArgs.PrevLogIndex + appendEntriesLen

						if curCommitLen >= rf.nextIndex[server] {
							rf.nextIndex[server] = curCommitLen +1
						}else {
							return
						}

						commitCond.L.Lock()
						defer commitCond.L.Unlock()

						commitNum += 1
						if commitFlag && commitNum > len(rf.peers)/2 {
							commitFlag = false
							DPrintf(rf.printLog, "info", "me:%2d term:%3d | curCommitLen:%3d  rf.commitIndex:%3d\n",
								rf.me, rf.currentTerm, curCommitLen , rf.commitIndex)

							if curCommitLen > rf.lastApplied {

								//本轮commit的日志长度大于leader当前的commit长度
								//假如原来日志长度为10
								//发送了1的日志，然后又发送了1~4的日志
								//先commit了1的日志，长度变11
								//然后接到1~4的commit，curCommitLen=14
								//curCommitLen和leader当前日志的差是3
								//所以leader只需要commit本次entries的后3个命令即可。

								for i := rf.lastApplied + 1; i <= curCommitLen; i++ {
									//leader将本条命令应用到状态机
									rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
									//通知client，本条命令成功commit
									//上面必须for循环，因为消息要按顺序执行
									DPrintf(rf.printLog, "info", "me:%2d term:%3d | Leader Commit:%4d OK, commitIndex:%3d\n",
										rf.me, rf.currentTerm, rf.log[i].Command, i)
								}

								rf.lastApplied = curCommitLen
								rf.commitIndex = curCommitLen

							}

						}

						rf.matchIndex[server] = curCommitLen

					} else {

						// append失败的两种原因:
						if reply.Term > curTerm {
							// 返回的term比 发送信息的时候的leader的term要大
							// 说明leader过期了
							rf.currentTerm = reply.Term
							rf.changeRole(Follower)
							// 暂时不知道新的leader,等待新的leader心跳
							rf.appendCh <- &AppendEntriesArgs{rf.currentTerm,
								-1, -1,-1,make([]Entries,0),-1}
						} else {
							// prevLogIndex 或者 prevLogTerm不匹配
							rf.nextIndex[server] = reply.ConflictIndex
							DPrintf(rf.printLog, "info", "me:%2d term:%3d | Msg to %3d append fail,decrease nextIndex to:%3d\n",
								rf.me, rf.currentTerm, server, rf.nextIndex[server])
						}
					}
				}

			}(followerID)
		}

		select {

			case <- rf.appendCh:
				//收到了别的leader的hearBeat,说明自己是过时的leader
				rf.changeRole(Follower)
				return
			case <- rf.voteCH:
				rf.changeRole(Follower)
				return
			case <- time.After(time.Duration(rf.heartBeatTimeout) * time.Millisecond):
				//do nothing

		}
	}

}




//Candidate
/**

1.follower超时,成为candidate
2.candidate获得超过半数的机器的确认,升级为leader
3.投票给别人,自己变回follower
4.收到leader的心跳,然后转为follower
5.定时器超时之后,重新投票

 */
func (rf *Raft) Candidate() {


	rf.mu.Lock()

	logLen := len(rf.log) - 1
	requestArgs := &RequestVoteArgs{rf.currentTerm, rf.me,
		logLen, rf.log[logLen].CurrentTerm}
	rf.persist()
	rf.mu.Unlock()

	//获得的选票 自己投给自己
	voteCount := 1
	winElection := make(chan bool)
	voteL := sync.Mutex{}
	voteFlag := true //获得过半选票后通知

	rf.resetTimeOut()
	for followerId,_ := range rf.peers {

		if followerId == rf.me {
			continue
		}

		// 并行向follower发起投票请求
		go func (server int) {
			reply := &RequestVoteReply{}
			if ok:= rf.sendRequestVote(server, requestArgs, reply);ok {
				//ok 表示仅仅代表得到回复
				if reply.VoteGranted {
					//收到投票
					voteL.Lock()
					voteCount += 1
					if voteFlag && voteCount > len(rf.peers)/2 {
						voteFlag = false
						voteL.Unlock()
						winElection <- true
					}else{
						voteL.Unlock()
					}
				}
			}
		}(followerId)
	}

	select{
	case <- rf.appendCh:

		//收到了heartBeat
		//DPrintf("info", "me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.leaderId)
		rf.changeRole(Follower)
		return
	case  <- rf.voteCH:
		// 投票过后
		//DPrintf("warn", "me:%2d term:%3d | role:%12s vote to candidate %3d\n", rf.me, rf.currentTerm, rf.role, args.candidateId)
		rf.changeRole(Follower)
		return
	case <- winElection:
		//赢得大选 变为leader
		rf.changeRole(Leader)
		return
	case <- rf.electionTimeOut.C:
		//选举超时 重新投票
		//DPrintf( "warn", "me:%2d term:%3d | candidate timeout!\n", rf.me, rf.currentTerm)
		rf.changeRole(Follower)
		return

	}
}

//Follower
func (rf *Raft) Follower () {

	for{
		rf.resetTimeOut()
		select {
		case <- rf.appendCh:
			//收到心跳信息
			//DPrintf("info", "me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.leaderId)
		case <-rf.voteCH:
			//投票给某人
			//DPrintf( "warn", "me:%2d term:%3d | role:%12s vote to candidate %3d\n", rf.me, rf.currentTerm, rf.role, args.candidateId)
		case <- rf.electionTimeOut.C:
			//超时
			//DPrintf( "warn", "me:%2d term:%3d | follower timeout!\n", rf.me, rf.currentTerm)
			rf.changeRole(Candidate)
			return
		}
	}
}




//
// example RequestVote RPC handler.
//
// 站在接收者的角度来看待这个函数
// args.xx 是candidate的属性
// rf.xx 是接收者的属性
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//如果接收者自己的任期都比候选人要新,就告诉他你TM洗洗睡吧
	if rf.currentTerm > args.Term {

		return
	}

	if rf.currentTerm == args.Term && rf.role ==Leader {
		//同一个任期leader忽略其他candidate发来的投票
		return
	}

	rf.currentTerm = args.Term

	//logEntries从下标1开始，log.Entries[0]是占位符
	//所以真实日志长度需要-1
	logLen := len(rf.log) - 1


	DPrintf(rf.printLog, "info", "me:%2d term:%3d curLogLen:%3d logTerm:%3d | candidate:%3d lastLogIndex:%3d lastLogTerm:%3d\n",
		rf.me, rf.currentTerm, logLen, rf.log[logLen].CurrentTerm, args.CandidateId, args.LastLogIndex, args.LastLogTerm)

	//检查参选人的日志是否比接收者的更加新
	// 先比较任期,任期相同就比较index
	if  args.LastLogTerm > rf.log[logLen].CurrentTerm || (args.LastLogTerm == rf.log[logLen].CurrentTerm && args.LastLogIndex >= logLen){


		if rf.votedThisTerm < args.Term {

			rf.votedThisTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			rf.persist()

			//投完票之后重启定时器
			go func() {
				rf.voteCH <- args
			}()
		}

	}
}


//站在follower的角度
//这里是follower收到leader的消息
//args 表示 leader的参数
func (rf *Raft) AppendEntries (args *AppendEntriesArgs, reply *AppendEntriesReply) {


	rf.mu.Lock()
	defer rf.mu.Unlock()
	//如果leader的任期小于follower的任期
	//那肯定是不能更新成功,并且回复给leader现在最新的任期
	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	logLen := len(rf.log) -1

	/**
	如果follower的日志小于args的日志,或者follower在args.prevLogIndex
	处的日志与arg不符合,返回false

	 */

	// 接收者日志 小于 leader 的prelogIndex
	logLess := logLen < args.PrevLogIndex
	//disMatch 定义: 日志索引相同 而且 之前已经接受过日志 而且rf在leader上一次发送的日志的任期不匹配
	logDismatch := !logLess  && rf.log[args.PrevLogIndex].CurrentTerm != args.PrevLogTerm

	if logLess || logDismatch {

		reply.Term = rf.currentTerm
		reply.Success = false

		//DPrintf("info", "me:%2d term:%3d | receive leader:[%3d] message but not match!\n", rf.me, rf.currentTerm, args.leaderId)


		if logDismatch {
			//把这个term的所有的日志回滚
			for index :=logLen-1;index>=0;index-- {
				if rf.log[index].CurrentTerm != rf.log[index+1].CurrentTerm {
					reply.ConflictIndex = index + 1
				}
			}
		}

		if logLess {
			reply.ConflictIndex = logLen + 1
		}

		DPrintf(rf.printLog, "info", "me:%2d term:%3d | receive leader:[%3d] message but not match!\n", rf.me, rf.currentTerm, args.LeaderId)


	} else {

		rf.currentTerm = args.Term
		reply.Success = true

		//如果存在一条日志索引和 prevLogIndex 相等,
		//但是任期和 prevLogItem 不相同的日志,
		//需要删除这条日志及所有后继日志。

		// 防止越界
		leng := min(logLen - args.PrevLogIndex, len(args.Entries))

		i := 0

		for ;i < leng; i++ {
			if args.Entries[i].CurrentTerm != rf.log[args.PrevLogIndex + i + 1].CurrentTerm {

				//删掉之前已经匹配的,重复的日志
				rf.log = rf.log[:args.PrevLogIndex + i + 1]
				break

			}
		}
		//如果 leader 复制的日志本地没有,则直接追加存储
		if i != len(args.Entries) {

			//更新follower的日志
			rf.log = append(rf.log, args.Entries[i:]...)

		}

		if args.LeaderCommit > rf.commitIndex {
			//记得rf.log[0]是占位符 因此长度要减一
			newcommitIndex := min(len(rf.log)-1, args.LeaderCommit)

			for i := rf.lastApplied +1 ; i <= newcommitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.log[i].Command, i}
			}

			rf.commitIndex = newcommitIndex
			rf.lastApplied = newcommitIndex
			rf.persist()
		}

		go func() {
			rf.appendCh <- args
		}()

	}

}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.role == Leader
	term = rf.currentTerm

	if isLeader {

		index = len(rf.log)
		rf.log = append(rf.log, Entries{rf.currentTerm, command})
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}



func (rf *Raft)changeRole(role string){

	switch role {
	case Leader:
		DPrintf(rf.printLog, "warn", "me:%2d term:%3d | %12s change role to Leader!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Leader
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		rf.votedThisTerm = rf.currentTerm
		rf.votedFor = rf.me //投票投给自己
		DPrintf(rf.printLog, "warn", "me:%2d term:%3d | %12s change role to candidate!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Candidate
	case Follower:
		DPrintf(rf.printLog, "warn", "me:%2d term:%3d | %12s change role to follower!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Follower
	}
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,

	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.role = Follower
	rf.printLog = false
	//LOG entry 从 1开始,0是占位符,没有意义
	rf.log = make([]Entries, 1)

	//term为-1 表示第一个leader的term编号为0,command也为int
	rf.log[0] = Entries{-1,-1}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendCh = make(chan *AppendEntriesArgs)
	rf.voteCH = make(chan *RequestVoteArgs)

	//设置每次随机数种子
	rand.Seed(time.Now().UnixNano())

	//心跳间隔
	rf.heartBeatTimeout = 100

	//DPrintf("Create a new server:[%3d]! term:[%3d]\n",rf.me, rf.currentTerm )
	go rf.run()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) run() {

	for{

		switch  rf.role {
		case Leader:
			rf.leader()
		case Candidate:
			rf.Candidate()
		case Follower:
			rf.Follower()
		}
	}
}


