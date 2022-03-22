package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// Add your RPC definitions here.
type AskForTask_Args struct {
}

type AskForMapTask_Reply struct {
	File string
	// reduce任务个数
	NumReduce int
	// map任务个数
	NumMap int
	// map任务分配到的文件次序
	FileOrder int
	// 表示是map任务还是reduce任务
	TaskType int
	// 如果是reduce任务的话对应的是哪个reduce任务
	ReduceOrder int
	// 所有任务均已完成
	AllFinished bool
}

// worker做完任务后发消息给coordinator
type InformFinished_Args struct {
	File        string
	ReduceOrder int
}
type InformFinished_Reply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
