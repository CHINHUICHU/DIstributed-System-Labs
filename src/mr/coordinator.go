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

type Coordinator struct {
	// Your definitions here.
	Files             []string // for retry map task
	FileNames         chan File
	NMap              int
	NReduce           int
	ReduceNumbers     chan int
	NMapTask          int
	MapTaskLock       sync.Mutex
	NReduceTask       int
	ReduceTaskLock    sync.Mutex
	MapStatus         []TaskStatus
	ReduceStatus      []TaskStatus
	MapStatusLocks    []sync.Mutex
	ReduceStatusLocks []sync.Mutex
	finishSetup       bool
}

type File struct {
	Name  string
	Index int
}

type TaskStatus struct {
	Time time.Time
	Done bool
}

func (c *Coordinator) processFiles(files []string) chan File {
	ch := make(chan File)
	for index, filename := range files {
		go func(f string, i int) {
			ch <- File{f, i}
		}(filename, index)
		c.Files = append(c.Files, filename)
	}
	return ch
}

func (c *Coordinator) generateReduceNumber(nReduce int) chan int {
	ch := make(chan int)
	for i := 0; i < nReduce; i += 1 {
		go func(i int) {
			ch <- i
		}(i)
	}
	return ch
}

func (c *Coordinator) DoTask(args *RpcArgs, reply *RpcReply) error {
	// coordinator should first check if all the map tasks are finished
	// if so, it should start the reduce tasks
	// if not, it should assign a map task to the worker
	if !c.finishSetup {
		return errors.New("not ready")
	}

	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	if c.NMapTask == c.NMap {
		// assign a reduce task to the worker
		return c.assignReduceTask(args, reply)
	}
	// assign a map task to the worker
	return c.assignMapTask(args, reply)
}

// assign map task to workers
func (c *Coordinator) assignMapTask(args *RpcArgs, reply *RpcReply) error {
	select {
	case file := <-c.FileNames:
		reply.Task = "map"
		fmt.Println(file.Name)
		reply.FileName = file.Name
		reply.MapNumber = file.Index
		reply.NReduce = c.NReduce
		reply.NMap = c.NMap
		fmt.Println("assign map task", file.Name, file.Index)
		// record get task time
		c.MapStatusLocks[file.Index].Lock()
		c.MapStatus[file.Index].Time = time.Now()
		c.MapStatusLocks[file.Index].Unlock()
		return nil
	default:
		return errors.New("no more file")
	}
}

// assign reduce task to workers
func (c *Coordinator) assignReduceTask(args *RpcArgs, reply *RpcReply) error {
	reply.Task = "reduce"
	select {
	case n := <-c.ReduceNumbers:
		reply.ReduceNumber = n
		reply.NMap = c.NMap
		reply.NReduce = c.NReduce
		fmt.Println("assign reduce task", n)
		// record get task time
		c.ReduceStatusLocks[n].Lock()
		c.ReduceStatus[n].Time = time.Now()
		c.ReduceStatusLocks[n].Unlock()
		return nil
	default:
		return errors.New("no more reduce task")
	}
}

// recieve task finished message from workers
func (c *Coordinator) TaskFinished(args *RpcArgs, reply *RpcReply) error {
	if args.Task == "map" {
		return c.mapTaskFinished(args, reply)
	}
	return c.reduceTaskFinished(args, reply)
}

// map task finished
func (c *Coordinator) mapTaskFinished(args *RpcArgs, reply *RpcReply) error {
	c.MapStatusLocks[args.MapNumber].Lock()
	defer c.MapStatusLocks[args.MapNumber].Unlock()
	if !c.MapStatus[args.MapNumber].Done {
		c.MapStatus[args.MapNumber].Time = time.Now()
		c.MapStatus[args.MapNumber].Done = true
		c.MapTaskLock.Lock()
		c.NMapTask += 1
		c.MapTaskLock.Unlock()
		return nil
	}
	return errors.New("map task already finished")
}

// reduce task finished
func (c *Coordinator) reduceTaskFinished(args *RpcArgs, reply *RpcReply) error {
	c.ReduceStatusLocks[args.ReduceNumber].Lock()
	defer c.ReduceStatusLocks[args.ReduceNumber].Unlock()
	if !c.ReduceStatus[args.ReduceNumber].Done {
		c.ReduceStatus[args.ReduceNumber].Time = time.Now()
		c.ReduceStatus[args.ReduceNumber].Done = true
		c.ReduceTaskLock.Lock()
		c.NReduceTask += 1
		c.ReduceTaskLock.Unlock()
		return nil
	}
	return errors.New("reduce task already finished")
}

// periodically check if worker is alive
func (c *Coordinator) checkTaskStatus() {
	for {
		time.Sleep(10 * time.Second)
		// check map task status
		c.checkMapTaskStatus()
		// check reduce task status after all map tasks are finished
		c.MapTaskLock.Lock()
		if c.NMapTask == c.NMap {
			c.checkReduceTaskStatus()
		}
		c.MapTaskLock.Unlock()
	}
}

// check map task status
func (c *Coordinator) checkMapTaskStatus() {
	fmt.Println("check map task status...")
	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	if c.NMapTask < c.NMap {
		for i := 0; i < c.NMap; i += 1 {
			c.MapStatusLocks[i].Lock()
			defer c.MapStatusLocks[i].Unlock()
			if !c.MapStatus[i].Done && time.Since(c.MapStatus[i].Time) > 10*time.Second {
				fmt.Println("map task timeout", i)
				go func(fileName string, i int) {
					c.FileNames <- File{fileName, i}
				}(c.Files[i], i)
			}
		}
	}
}

// check reduce task status
func (c *Coordinator) checkReduceTaskStatus() {
	fmt.Println("check reduce task status...")
	c.ReduceTaskLock.Lock()
	defer c.ReduceTaskLock.Unlock()
	if c.NReduceTask < c.NReduce {
		for i := 0; i < c.NReduce; i += 1 {
			c.ReduceStatusLocks[i].Lock()
			defer c.ReduceStatusLocks[i].Unlock()
			if !c.ReduceStatus[i].Done && time.Since(c.ReduceStatus[i].Time) > 10*time.Second {
				fmt.Println("reduce task timeout", i)
				go func(i int) {
					c.ReduceNumbers <- i
				}(i)
			}
		}
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
	c.ReduceTaskLock.Lock()
	defer c.ReduceTaskLock.Unlock()
	if c.NReduceTask == c.NReduce {
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
	c.NMap = len(files)
	c.Files = append(c.Files, files...)
	c.FileNames = c.processFiles(files)
	c.NReduce = nReduce
	c.ReduceNumbers = c.generateReduceNumber(nReduce)
	c.NMapTask = 0
	c.NReduceTask = 0
	c.MapStatus = make([]TaskStatus, len(files))
	c.MapStatusLocks = make([]sync.Mutex, len(files))
	c.ReduceStatus = make([]TaskStatus, nReduce)
	c.ReduceStatusLocks = make([]sync.Mutex, nReduce)
	c.finishSetup = true
	fmt.Println("coordinator setup finished")
	c.server()
	go c.checkTaskStatus()

	return &c
}
