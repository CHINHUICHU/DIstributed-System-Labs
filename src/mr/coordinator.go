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
	Files              []string // for retry map task
	FileNames          chan File
	NMap               int
	NReduce            int
	ReduceNumbers      chan int
	NMapTask           int
	MapTaskLock        sync.Mutex
	NReduceTask        int
	ReduceTaskLock     sync.Mutex
	NMapLock           sync.Mutex
	MapTimeRecords     []time.Time
	ReduceTimeRecords  []time.Time
	MapFinishStatus    []bool
	ReduceFinishStatus []bool
	MapRecordLocks     []sync.Mutex
	ReduceRecordLocks  []sync.Mutex
	finishSetup        bool
}

type File struct {
	Name  string
	Index int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) processFiles() chan File {
	ch := make(chan File)
	for index, filename := range os.Args[1:] {
		go func(f string, i int) {
			ch <- File{f, i}
		}(filename, index)
		c.NMapLock.Lock()
		c.NMap += 1
		c.NMapLock.Unlock()
		c.Files = append(c.Files, filename)
	}
	return ch
}

func (c *Coordinator) DoMapTask(args *RpcArgs, reply *RpcReply) error {
	// file name should be received from the channel
	if c.finishSetup {
		select {
		case file := <-c.FileNames:
			fmt.Println(file.Name)
			reply.FileName = file.Name
			reply.MapNumber = file.Index
			c.MapRecordLocks[args.MapNumber].Lock()
			c.MapTimeRecords[args.MapNumber] = time.Now()
			c.MapRecordLocks[args.MapNumber].Unlock()
			reply.NReduce = c.NReduce
			reply.NMap = c.NMap
			return nil
		default:
			return errors.New("no more file")
		}
	} else {
		return errors.New("not ready")
	}
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

func (c *Coordinator) MapNotify(args *RpcArgs, reply *RpcReply) error {
	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	if c.NMapTask == c.NMap {
		return errors.New("mapper notify finished")
	}
	if !c.MapFinishStatus[args.MapNumber] {
		c.MapRecordLocks[args.MapNumber].Lock()
		c.MapTimeRecords[args.MapNumber] = time.Now()
		c.MapFinishStatus[args.MapNumber] = true
		c.MapRecordLocks[args.MapNumber].Unlock()
		c.NMapTask += 1
		return nil
	} else {
		return errors.New("map task already finished")
	}

}
func (c *Coordinator) ReduceNotify(args *RpcArgs, reply *RpcReply) error {
	c.ReduceTaskLock.Lock()
	defer c.ReduceTaskLock.Unlock()
	if c.NReduceTask == c.NReduce {
		return errors.New("reducer notify finished")
	}
	if !c.ReduceFinishStatus[args.ReduceNumber] {
		c.ReduceRecordLocks[args.ReduceNumber].Lock()
		c.ReduceTimeRecords[args.ReduceNumber] = time.Now()
		c.ReduceFinishStatus[args.ReduceNumber] = true
		c.ReduceRecordLocks[args.ReduceNumber].Unlock()
		c.NReduceTask += 1
		return nil
	} else {
		return errors.New("reduce task already finished")
	}
}

func (c *Coordinator) DoReduceTask(args *RpcArgs, reply *RpcReply) error {
	c.MapTaskLock.Lock()
	defer c.MapTaskLock.Unlock()
	if c.NMapTask == c.NMap {
		reply.StartReduce = true
		select {
		case n := <-c.ReduceNumbers:
			reply.ReduceNumber = n
			c.ReduceRecordLocks[args.ReduceNumber].Lock()
			c.ReduceTimeRecords[args.ReduceNumber] = time.Now()
			c.ReduceRecordLocks[args.ReduceNumber].Unlock()
			reply.NMap = c.NMap
			return nil
		default:
			return errors.New("no reduce task left")
		}
	}
	reply.StartReduce = false
	return nil

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

// periodically, check if the MapTimeRecords and time.Now differ more than 10 seconds
// If so, retry the map task by sending the file name to the channel
func (c *Coordinator) checkMapStatus() {
	for {
		finishedMap := 0
		for i, t := range c.MapTimeRecords {
			c.MapRecordLocks[i].Lock()
			if time.Now().Sub(t) > 10*time.Second && !c.MapFinishStatus[i] {
				c.MapTimeRecords[i] = time.Now()
				c.FileNames <- File{c.Files[i], i}
			} else if c.MapFinishStatus[i] {
				finishedMap += 1
			}
			c.MapRecordLocks[i].Unlock()
		}
		fmt.Println("check map status", finishedMap, "mappers finished....")
		if finishedMap == c.NMap {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// periodically, check if the ReduceTimeRecords and time.Now differ more than 10 seconds
// If so, retry the reduce task by sending the reduce number to the channel
func (c *Coordinator) checkReduceStatus() {
	for {
		finishedReduce := 0
		for i, t := range c.ReduceTimeRecords {
			c.ReduceRecordLocks[i].Lock()
			if time.Now().Sub(t) > 10*time.Second && !c.ReduceFinishStatus[i] {
				fmt.Println("reduce task", i, "failed, retry")
				c.ReduceTimeRecords[i] = time.Now()
				c.ReduceNumbers <- i
			} else if c.ReduceFinishStatus[i] {
				finishedReduce += 1
			}
			c.ReduceRecordLocks[i].Unlock()
		}
		fmt.Println("check reduce status", finishedReduce, "reducers finished....")

		if finishedReduce == c.NReduce {
			break
		}
		time.Sleep(10 * time.Second)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NMap = 0
	c.Files = make([]string, 0)
	c.FileNames = c.processFiles()
	c.NReduce = nReduce
	c.ReduceNumbers = c.generateReduceNumber(nReduce)
	c.NMapTask = 0
	c.NReduceTask = 0
	c.MapTimeRecords = make([]time.Time, c.NMap)
	c.MapRecordLocks = make([]sync.Mutex, c.NMap)
	c.MapFinishStatus = make([]bool, c.NMap)
	for i := range c.MapTimeRecords {
		go func(i int) {
			c.MapRecordLocks[i].Lock()
			c.MapTimeRecords[i] = time.Now()
			c.MapRecordLocks[i].Unlock()
		}(i)
	}
	c.ReduceTimeRecords = make([]time.Time, c.NReduce)
	c.ReduceRecordLocks = make([]sync.Mutex, c.NReduce)
	c.ReduceFinishStatus = make([]bool, c.NReduce)
	for i := range c.ReduceTimeRecords {
		go func(i int) {
			c.ReduceRecordLocks[i].Lock()
			c.ReduceTimeRecords[i] = time.Now()
			c.ReduceRecordLocks[i].Unlock()
		}(i)
	}
	c.finishSetup = true
	c.server()
	c.checkMapStatus()
	c.checkReduceStatus()

	return &c
}
