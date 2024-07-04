package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := Reply{}
		call("Coordinator.RequestTask", &Args{Pid: os.Getpid()}, &reply)

		switch reply.TaskType {
		case MapTask:
			HandleMapTask(&reply, mapf)
		case ReduceTask:
			HandleReduceTask(&reply, reducef)
		case SleepTask:
			time.Sleep(1 * time.Second)
		case QuitTask:
			return
		}
	}
}

func HandleMapTask(reply *Reply, mapf func(string, string) []KeyValue) {
	/*
		intermediate := []mr.KeyValue{}
		for _, filename := range os.Args[2:] {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}
	*/

	intermediate := make([][]KeyValue, reply.TaskNReduce)
	file, err := os.Open(reply.TaskInput)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskInput)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskInput)
		return
	}
	file.Close()
	kva := mapf(reply.TaskInput, string(content))
	/*
		单体式 ihash恒为0
		ihash[0] => kva[*]
		所以单体式Reduce的output文件也只有mr-out-0
		分布式 ihash∈[0, NReduce - 1]
		ihash[0] => kva[ihash(key) = 0]
		ihash[1] => kva[ihash(key) = 1]
		...
		ihash[NReduce - 1] => ihash[ihash(key) = NReduce - 1]
	*/
	for _, kv := range kva {
		hash := ihash(kv.Key) % reply.TaskNReduce
		intermediate[hash] = append(intermediate[hash], kv)
	}

	/*
		sort.Sort(ByKey(intermediate))
		Map不需要，因为排序应该放在Reduce操作中
	*/

	/*
		oname := "mr-out-0"
		ofile, _ := os.Create(oname)
		ofile.Close()
		这个是Reduce的

		Map应该是"mr-${MapTaskID}-${ihash(Key)}""
	*/
	for hash, kva := range intermediate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, hash)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("cannot create file %v", oname)
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kva {
			enc.Encode(kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	args := Args{
		Pid:        os.Getpid(),
		TaskID:     reply.TaskID,
		TaskStatus: Done,
		TaskType:   MapTask,
	}
	call("Coordinator.ReportTask", &args, &Reply{})
}

func HandleReduceTask(reply *Reply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	/*
		遍历所有"mr-*-${ihash(Key)}"
	*/
	intermediateFilenames := getFilenames(reply.TaskID, reply.MapTaskCount)
	for _, filename := range intermediateFilenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return
		}
		decorder := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := decorder.Decode(&kv); err == io.EOF {
				break
			}
			intermediate = append(intermediate, kv)
		}
		// 也就是说如果在Map操作排序就是在做无用功，此时为局部有序，整体无序
		file.Close()
	}

	/*
		sort.Sort(ByKey(intermediate))
	*/
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	/*
		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

		这里应该是"mr-out-${ihash(Key)}"
	*/

	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		return
	}

	/*
		只要实现intermediate:
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}
	*/
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	os.Rename(ofile.Name(), oname)

	args := Args{
		Pid:        os.Getpid(),
		TaskID:     reply.TaskID,
		TaskStatus: Done,
		TaskType:   ReduceTask,
	}
	call("Coordinator.ReportTask", &args, &Reply{})
}

func getFilenames(hash int, MapTaskCount int) []string {
	filenames := []string{}
	for i := 0; i < MapTaskCount; i++ {
		filenames = append(filenames, fmt.Sprintf("mr-%d-%d", i, hash))
	}
	return filenames
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
