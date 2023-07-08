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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RpcArgs struct {
	Task       TaskType
	TaskNumber int
}

// need default value
type RpcReply struct {
	Task       TaskType
	TaskNumber int
	FileName   string
	Total      map[TaskType]int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	// fmt.Println("coordinator sock", s)

	return s
}
