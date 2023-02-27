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

	args := RpcArgs{}
	// record map start time for each map task
	reply := RpcReply{}
	for {
		if call("Coordinator.DoTask", &args, &reply) {
			// check the task type
			if reply.Task == "map" {
				doMap(mapf, reply.FileName, reply.NReduce, reply.MapNumber)
				args.Task = "map"
				args.MapNumber = reply.MapNumber
				call("Coordinator.TaskFinished", &args, &reply)
			} else {
				doReduce(reducef, reply.NMap, reply.ReduceNumber)
				args.Task = "reduce"
				args.ReduceNumber = reply.ReduceNumber
				call("Coordinator.TaskFinished", &args, &reply)
			}
		} else {
			// sleep for 1 seconds and retry
			time.Sleep(1 * time.Second)
			reply = RpcReply{}
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, fileName string, NReduce int, mapNumber int) {
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
	// 3. call mapf
	kva := mapf(fileName, string(content))

	intermediate := make([]*os.File, NReduce)
	encoders := make([]*json.Encoder, NReduce)
	splitKv := make([][]KeyValue, NReduce)

	// 4. create intermediate files
	for i := 0; i < NReduce; i += 1 {
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
				fmt.Println("encode failed")
			}
		}
		intermediate[i].Close()
	}

	// 7. rename temp files to final files
	for i := 0; i < NReduce; i += 1 {
		finalFilename := fmt.Sprintf("mr-%d-%d", mapNumber+1, i)
		err := os.Rename(intermediate[i].Name(), finalFilename)
		if err != nil {
			log.Fatalf("cannot rename %v", intermediate[i].Name())
		}
	}

}

func doReduce(reducef func(string, []string) string, NMap int, reduceNumber int) {
	kva := []KeyValue{}
	for i := 0; i < NMap; i += 1 {
		// 1. read intermediate files
		filename := fmt.Sprintf("mr-%d-%d", i+1, reduceNumber)
		file, err := os.Open(filename)
		if err != nil {
			panic(err)
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
	// 5. rename temp file to final file
	for i := 0; i < NMap; i += 1 {
		filename := fmt.Sprintf("mr-%d-%d", i+1, reduceNumber)
		if err := os.Remove(filename); err != nil {
			log.Fatalf("cannot remove %v", filename)
		}
	}
	err := os.Rename(ofile.Name(), fmt.Sprintf("mr-out-%d", reduceNumber))
	if err != nil {
		log.Fatalf("cannot rename %v", ofile.Name())
	}
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
