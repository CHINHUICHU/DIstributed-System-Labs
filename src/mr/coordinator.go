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
)

type Coordinator struct {
	// Your definitions here.
	FileNames      chan File
	NMap           int
	NReduce        int
	ReduceNumbers  chan int
	NMapTask       int
	MapTaskLock    sync.Mutex
	NReduceTask    int
	ReduceTaskLock sync.Mutex
}

type File struct {
	Name  string
	Index int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) putFilesToChannel() chan File {
	ch := make(chan File)
	for index, filename := range os.Args[1:] {
		go func(f string, i int) {
			ch <- File{f, i}
		}(filename, index)
		c.NMap += 1
	}
	return ch
}

func (c *Coordinator) GetInitialData(args *RpcArgs, reply *RpcReply) error {
	// file name should be received from the channel
	select {
	case file := <-c.FileNames:
		fmt.Println(file.Name)
		reply.FileName = file.Name
		reply.MapNumber = file.Index
		reply.NReduce = c.NReduce
		reply.NMap = c.NMap
		return nil
	default:
		return errors.New("no more file")
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
	if c.NMapTask <= 0 {
		return errors.New("mapper notify finished")
	}
	c.NMapTask -= 1
	c.MapTaskLock.Unlock()
	return nil

}

func (c *Coordinator) StartReduce(args *RpcArgs, reply *RpcReply) error {
	if c.NMapTask == 0 {
		reply.StartReduce = true
	} else {
		reply.StartReduce = false
	}
	return nil
}

func (c *Coordinator) GetReduceNumber(args *RpcArgs, reply *RpcReply) error {
	select {
	case n := <-c.ReduceNumbers:
		c.ReduceTaskLock.Lock()
		reply.ReduceNumber = n
		c.NReduceTask -= 1
		c.ReduceTaskLock.Unlock()
		return nil
	default:
		return errors.New("no reduce task left")
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
	if c.NReduceTask == 0 {
		ret = true
	}
	c.ReduceTaskLock.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NMap = 0
	c.FileNames = c.putFilesToChannel()
	c.NReduce = nReduce
	c.ReduceNumbers = c.generateReduceNumber(nReduce)
	c.NMapTask = c.NMap * c.NReduce
	c.NReduceTask = c.NReduce

	c.server()
	return &c
}
