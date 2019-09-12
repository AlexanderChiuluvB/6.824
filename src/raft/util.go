package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//使得每个follower的计时器的时间都是随机的,防止同时选举出两个leader的情况
func (rf *Raft) getRandTime() int {

	baseTime := 300
	randTime := 150
	timeout := baseTime + rand.Intn(randTime)

	return timeout
}

//重置计时器
func (rf *Raft) resetTimeOut() {
	if rf.electionTimeOut == nil {
		rf.electionTimeOut = time.NewTimer(time.Duration(rf.getRandTime())*time.Millisecond)
	} else {
		rf.electionTimeOut.Reset(time.Duration(rf.getRandTime())*time.Millisecond)
	}
}


func min(a int, b int) int {

	if a > b {
		return b
	}

	return a

}



func max(a int, b int) int {


	if a > b {
		return a
	}

	return b
}
