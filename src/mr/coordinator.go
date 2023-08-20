package mr

import (
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
	Finish TaskType = "finish"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	files    []string // for retry map task
	tracker  map[TaskType]map[int]Record
	todos    map[TaskType]chan int
	total    map[TaskType]int
	finished map[TaskType]int
}

type Record struct {
	time     time.Time
	done     bool
	assigned bool
}

func (c *Coordinator) DoTask(args *RpcArgs, reply *RpcReply) error {
	reply.Total = c.total

	fmt.Printf("worker %d request task from coordinator time %v\n", args.WorkerID, timestamp())

	defer func() {
		if reply.Task != Finish {
			c.Lock()
			c.tracker[reply.Task][reply.TaskNumber] = Record{
				assigned: true,
				time:     time.Now(),
			}
			c.Unlock()
		}
	}()

	if !c.isFinished(Map) {
		number := <-c.todos[Map]
		reply.Task = Map
		reply.FileName = c.files[number]
		reply.TaskNumber = number
		fmt.Printf("coordinator assign worker %v map task with number %v, time %v\n", args.WorkerID, reply.TaskNumber, timestamp())
	} else if !c.isFinished(Reduce) {
		number := <-c.todos[Reduce]
		reply.Task = Reduce
		reply.TaskNumber = number
		fmt.Printf("coordinator assign worker %v reduce task with number %v, time %v\n", args.WorkerID, reply.TaskNumber, timestamp())
	}
	fmt.Printf("coordinator assign worker %v no task time %v\n", args.WorkerID, timestamp())
	if c.isFinished(Map) && c.isFinished(Reduce) {
		fmt.Printf("all task finished!!!!!, worker id %v\n", args.WorkerID)
		reply.Task = Finish
	}
	return nil
}

// receive task finished message from workers
func (c *Coordinator) FinishTask(args *RpcArgs, reply *RpcReply) error {
	c.Lock()
	defer c.Unlock()
	if record, ok := c.tracker[args.Task][args.TaskNumber]; ok && !record.done {
		record.done = true
		c.tracker[args.Task][args.TaskNumber] = record
		fmt.Printf("task type %v, task number %v, finished, time %v\n", args.Task, args.TaskNumber, timestamp())
		c.finished[args.Task]++
	}
	return nil
}

// periodically check if worker is alive
func (c *Coordinator) checkTaskStatus() {
	time.Sleep(10 * time.Second)
	for {
		c.Lock()
		for tasktype, tracker := range c.tracker {
			for n, record := range tracker {
				if record.assigned && !record.done && time.Since(record.time) > 10*time.Second {
					record.assigned = false
					c.tracker[tasktype][n] = record
					go func(n int, task TaskType) {
						c.todos[task] <- n
					}(n, tasktype)
				}
			}
		}
		c.Unlock()
		time.Sleep(2 * time.Second)
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
	if c.isFinished(Reduce) {
		fmt.Print("all reduce tasks finished, coordinator done\n")
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

	c.total = map[TaskType]int{
		Map:    len(files),
		Reduce: nReduce,
	}

	c.todos = map[TaskType]chan int{
		Map:    make(chan int, c.total[Map]),
		Reduce: make(chan int, c.total[Reduce]),
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

	c.finished = map[TaskType]int{
		Map:    0,
		Reduce: 0,
	}

	c.tracker = map[TaskType]map[int]Record{
		Map:    make(map[int]Record),
		Reduce: make(map[int]Record),
	}

	for key, value := range c.total {
		for i := 0; i < value; i++ {
			c.tracker[key][i] = Record{
				time: time.Now(),
			}
		}
	}

	c.server()
	go c.checkTaskStatus()

	return &c
}

func (c *Coordinator) isFinished(task TaskType) bool {
	c.Lock()
	defer c.Unlock()
	return c.finished[task] == c.total[task]
}
