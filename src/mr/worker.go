package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		args := &RpcArgs{}
		reply := &RpcReply{}
		pid := os.Getpid()
		args.WorkerID = pid
		fmt.Printf("worker (pid=%d) request task, time %v\n", pid, timestamp())
		call("Coordinator.DoTask", &args, &reply)
		fmt.Printf("worker (pid=%d) got task %v, number %v, time %v\n", pid, reply.Task, reply.TaskNumber, timestamp())
		if reply.Task == Map {
			mapper(mapf, args, reply)
		} else if reply.Task == Reduce {
			reducer(reducef, args, reply)
		} else {
			fmt.Printf("worker %v exit\n", pid)
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func mapper(mapf func(string, string) []KeyValue, args *RpcArgs, reply *RpcReply) {
	fileName, NReduce, mapNumber := reply.FileName, reply.Total[Reduce], reply.TaskNumber
	// 1. read file
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	// 2. read file content
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	fmt.Printf("Mapper %v timestamp before call mapf, time %v\n", mapNumber, timestamp())

	// 3. call mapf
	kva := mapf(fileName, string(content))
	fmt.Printf("Mapper %v timestamp after call mapf, time %v\n", mapNumber, timestamp())

	intermediate := make([]*os.File, NReduce)
	encoders := make([]*json.Encoder, NReduce)
	splitKv := make([][]KeyValue, NReduce)

	// 4. create intermediate files
	for i := 0; i < NReduce; i++ {
		tempFilename := fmt.Sprintf("temp-mr-%d-%d", mapNumber+1, i)
		tempFile, err := ioutil.TempFile("", tempFilename)
		if err != nil {
			log.Fatalf("cannot create temp file %v", tempFilename)
		}
		intermediate[i] = tempFile
		encoders[i] = json.NewEncoder(tempFile)
	}

	// 5. split kv into NReduce buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % NReduce
		splitKv[bucket] = append(splitKv[bucket], kv)
	}

	// 6. write kv to intermediate files
	for i, bucket := range splitKv {
		for _, kv := range bucket {
			err := encoders[i].Encode(&kv)
			if err != nil {
				fmt.Printf("encode failed\n")
			}
		}
		intermediate[i].Close()
	}

	// 7. rename temp files to final files
	for i := 0; i < NReduce; i++ {
		finalFilename := fmt.Sprintf("mr-%d-%d", mapNumber+1, i)
		err := os.Rename(intermediate[i].Name(), finalFilename)
		if err != nil {
			fmt.Printf("cannot rename %v", intermediate[i].Name())
		}
	}

	args = &RpcArgs{
		Task:       Map,
		TaskNumber: mapNumber,
	}
	reply = &RpcReply{}
	call("Coordinator.FinishTask", &args, &reply)

}

func reducer(reducef func(string, []string) string, args *RpcArgs, reply *RpcReply) {
	NMap, reduceNumber := reply.Total[Map], reply.TaskNumber
	kva := []KeyValue{}
	for i := 0; i < NMap; i++ {
		// 1. read intermediate files
		filename := fmt.Sprintf("mr-%d-%d", i+1, reduceNumber)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Printf("reducer can not open, err %v\n", err)
			return
		}
		// 2. decode intermediate files
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// EOF
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// 3. sort by key
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("temp-mr-out-%d", reduceNumber)
	ofile, _ := ioutil.TempFile("", oname)
	fmt.Printf("REDUCER %v timestamp before call reducef, time %v\n", reduceNumber, timestamp())

	// 4. call reducef
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	fmt.Printf("REDUCER %v timestamp after call reducef, time %v\n", reduceNumber, timestamp())

	// 5. rename temp file to final file
	for i := 0; i < NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i+1, reduceNumber)
		if err := os.Remove(filename); err != nil {
			log.Fatalf("cannot remove %v", filename)
		}
	}

	err := os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceNumber))
	if err != nil {
		log.Fatalf("cannot rename %v", ofile.Name())
	}
	args = &RpcArgs{
		Task:       Reduce,
		TaskNumber: reduceNumber,
	}
	reply = &RpcReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
}

var t0 = time.Now()

func timestamp() int64 {
	return time.Since(t0).Abs().Milliseconds()
}
