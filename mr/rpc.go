package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Map = iota // coordinator and worker use
	Reduce     // coordinator and worker use
	Wait       // worker use
	Exit       // coordinator use
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Request struct {
}

type Task struct {
	Id            int // Task的id
	Type          int // Task的类型[Map/Reduce]
	NMap          int
	NReduce       int
	FileName      string   // Map阶段使用
	Intermediates []string // Reduce阶段使用
}

type Result struct {
	Id            int // Task的id
	Type          int // Task的类型[Map/Reduce]
	Intermediates []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
