package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType string

const (
	Map    TaskType = "map"
	Reduce TaskType = "reduce"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	files    []string // for retry map task
	tracker  map[TaskType]map[int]bool
	todos    map[TaskType]chan int
	total    map[TaskType]int
	finished map[TaskType]int
}

func (c *Coordinator) DoTask(args *RpcArgs, reply *RpcReply) error {
	reply.Total = c.total

	defer func() {
		c.Lock()
		if reply.Task != "" {
			c.tracker[reply.Task][reply.TaskNumber] = false
		}
		c.Unlock()
	}()

	if !c.mapFinished() {
		number := <-c.todos[Map]
		reply.Task = Map
		reply.FileName = c.files[number]
		reply.TaskNumber = number
		return nil
	} else if !c.reduceFinished() {
		number := <-c.todos[Reduce]
		reply.Task = Reduce
		reply.TaskNumber = number
		return nil
	} else {
		return errors.New("all task done")
	}
}

// receive task finished message from workers
func (c *Coordinator) FinishTask(args *RpcArgs, reply *RpcReply) error {
	c.Lock()
	defer c.Unlock()
	if !c.tracker[args.Task][args.TaskNumber] {
		c.tracker[args.Task][args.TaskNumber] = true
		fmt.Printf("task type %v, task number %v, finished\n", args.Task, args.TaskNumber)
		c.finished[args.Task]++
		return nil
	}
	return fmt.Errorf("task type: %s index: %d task already finished", args.Task, args.TaskNumber)
}

// periodically check if worker is alive
func (c *Coordinator) checkTaskStatus() {
	for {
		time.Sleep(10 * time.Second)
		c.Lock()
		for key, tracker := range c.tracker {
			for n, done := range tracker {
				if !done {
					fmt.Printf("task type %v, task number %v failed, retry\n", key, n)
					go func(n int, task TaskType) {
						c.todos[task] <- n
					}(n, key)
				}
			}
		}
		c.Unlock()
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// periodically, check if the NReduceTask equals to 0
	// if so, return true
	if c.reduceFinished() {
		fmt.Println("all reduce tasks finished, coordinator done")
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = append(c.files, files...)
	c.todos = map[TaskType]chan int{
		Map:    make(chan int),
		Reduce: make(chan int),
	}

	for index := range files {
		go func(i int) {
			c.todos[Map] <- i
		}(index)
	}

	for i := 0; i < nReduce; i++ {
		go func(i int) {
			c.todos[Reduce] <- i
		}(i)
	}

	c.total = map[TaskType]int{
		Map:    len(files),
		Reduce: nReduce,
	}

	c.finished = map[TaskType]int{
		Map:    0,
		Reduce: 0,
	}

	c.tracker = map[TaskType]map[int]bool{
		Map:    make(map[int]bool),
		Reduce: make(map[int]bool),
	}

	c.server()
	go c.checkTaskStatus()

	return &c
}

func (c *Coordinator) mapFinished() bool {
	c.Lock()
	defer c.Unlock()
	return c.finished[Map] == c.total[Map]
}

func (c *Coordinator) reduceFinished() bool {
	c.Lock()
	defer c.Unlock()
	return c.finished[Reduce] == c.total[Reduce]
}
