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
	"strconv"
	"sync"
)

//TODO:backup with atomic Rename

var cv sync.Cond = *sync.NewCond(&sync.Mutex{})

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {

		status := Status{}
		ok := call("Coordinator.All_tasks_finish", &status, &status)
		if !ok {
			log.Fatalf("All_tasks_finish call failed\n")
		}
		if status.Status {
			break
		}
		reply := Reply{}
		for {
			reply = worker_asking_for_task(mapf, reducef)
			if !reply.waiting {
				break
			}
		}
		if (reply == Reply{}) {
			continue
		}

		if reply.Identity == map_task {
			implementation_of_map_task(&reply, mapf)
		} else {
			implementation_of_reduce_task(&reply, reducef)
		}
		worker_can_be_completed(&reply)
	}

}

func worker_asking_for_task(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) Reply {

	args := Args{}
	reply := Reply{}
	ok := call("Coordinator.Init", &args, &reply)

	if ok {
		return reply
	} else {
		log.Fatalf("Init failed!\n")
		return Reply{}
	}
}

// func is_file_exist(filename string) bool {
// 	_, err := os.Stat(filename)
// 	if err == nil {
// 		return true
// 	} else {
// 		return false
// 	}
// }

func implementation_of_map_task(worker *Reply, mapf func(string, string) []KeyValue) {
	//read files
	file, err := os.Open(worker.Filename)
	if err != nil {
		log.Fatalf("map cannot open %v", worker.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("map cannot read %v", worker.Filename)
	}
	file.Close()
	intermediate := mapf(worker.Filename, string(content))

	//write intermediate into disk

	intermediate_file_array := make([][]KeyValue, worker.NReduce)
	for _, kv := range intermediate {

		// println("map", kv.Key, kv.Value)

		output_number := ihash(kv.Key) % worker.NReduce
		intermediate_file_array[output_number] = append(intermediate_file_array[output_number], kv)
	}

	for i := 0; i < worker.NReduce; i++ {

		intermediate_filename := "mr-" + strconv.Itoa(worker.Id) + "-" + strconv.Itoa(i)

		file, _ := os.CreateTemp(os.TempDir(), "map tempfile")

		//TODO test
		// println(intermediate_filename)

		enc := json.NewEncoder(file)
		for _, kv := range intermediate_file_array[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("intermediate error")
			}
		}
		file.Close()

		os.Rename(file.Name(), intermediate_filename)

	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func implementation_of_reduce_task(worker *Reply, reducef func(string, []string) string) {
	//if all map tasks finish
	cv.L.Lock()
	for !all_map_tasks_are_finished() {
		// println(worker.Id)
		cv.Wait()
	}
	// println(worker.Id, worker.NMap)
	cv.Signal()
	cv.L.Unlock()
	intermediate := ByKey{}
	// println(worker.Id, worker.NMap)
	for i := 0; i < worker.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(worker.Id-worker.NMap)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("reduce cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}
	sort.Sort(intermediate)

	oname := "mr-out-" + strconv.Itoa(worker.Id-worker.NMap)
	ofile, _ := os.CreateTemp(os.TempDir(), "reduce tempfile")

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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

		// println(intermediate[i].Key, output)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	os.Rename(ofile.Name(), oname)
}

func all_map_tasks_are_finished() bool {
	reply := Status{}
	ok := call("Coordinator.All_map_tasks_finish", &reply, &reply)
	if !ok {
		log.Fatalf("All_map_tasks_finish call failed\n")
	}
	return reply.Status
}

func worker_can_be_completed(reply *Reply) {
	ok := call("Coordinator.Worker_is_completed", &reply, &reply)
	if !ok {
		log.Fatalf("Worker_is_completed call failed\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
