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
	reply := RpcReply{}
	for call("Coordinator.DoMapTask", &args, &reply) {

		file, err := os.Open(reply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.FileName)
		}

		file.Close()

		kva := mapf(reply.FileName, string(content))

		intermediate := make([]*os.File, reply.NReduce)
		encoders := make([]*json.Encoder, reply.NReduce)
		splitKv := make([][]KeyValue, reply.NReduce)

		// 4. create intermediate files
		for i := 0; i < reply.NReduce; i += 1 {
			filename := fmt.Sprintf("mr-%d-%d", reply.MapNumber, i)
			file, err := os.Create(filename)
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			intermediate[i] = file
			encoders[i] = json.NewEncoder(file)
		}

		// split key value pairs into buckets
		for _, kv := range kva {
			bucket := ihash(kv.Key) % reply.NReduce
			splitKv[bucket] = append(splitKv[bucket], kv)
		}

		// 5. write kv to intermediate files
		for i, bucket := range splitKv {
			for _, kv := range bucket {
				err := encoders[i].Encode(&kv)
				if err != nil {
					fmt.Println("encode failed")
				}
			}
			intermediate[i].Close()
			call("Coordinator.MapNotify", &args, &reply)
		}
		reply = RpcReply{}
	}

	reply = RpcReply{}
	for call("Coordinator.DoReduceTask", &args, &reply) {
		if !reply.StartReduce {
			time.Sleep(time.Second * 3)
			fmt.Print("wait for reduce start\n")
			reply = RpcReply{}
		} else {
			kva := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceNumber)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
				err = os.Remove(filename)
				if err != nil {
					log.Fatalf("cannot remove %v", filename)
				}
			}
			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%d", reply.ReduceNumber)
			ofile, _ := os.Create(oname)

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
			call("Coordinator.ReduceNotify", &args, &reply)
			fmt.Println("reduce done")
		}
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
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
