package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	idle = iota
	in_progress
	completed
)

const (
	map_task = iota
	reduce_task
)

type Coordinator struct {
	// Your definitions here.
	status                      []int
	identity                    []int
	start_time                  []time.Time
	nMap                        int
	nReduce                     int
	files                       []string
	mutex                       sync.Mutex
	count_of_completed_task     int
	count_of_completed_map_task int
	// cv                          sync.Cond
	map_channel    chan int
	reduce_channel chan int
	map_task_done  bool
	done           bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Init(args *Args, reply *Reply) error {

	if len(c.map_channel) > 0 {
		reply.Id = <-c.map_channel
	} else if !c.map_task_done {
		// println(1111, len(c.map_channel))
		// time.Sleep(time.Millisecond * 200)
		reply.waiting = true
		return nil
	} else if !c.done {
		if len(c.reduce_channel) > 0 {
			reply.Id = <-c.reduce_channel
		} else {
			// println(2222, len(c.reduce_channel), c.done)
			// time.Sleep(time.Millisecond * 200)
			reply.waiting = true
			return nil
		}
	} else {
		return nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// println(reply.Id)

	c.start_time[reply.Id] = time.Now()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	if reply.Id < c.nMap {
		c.identity[reply.Id] = map_task
		c.status[reply.Id] = in_progress
		reply.Identity = map_task
		reply.Filename = c.files[reply.Id]
	} else {
		c.identity[reply.Id] = reduce_task
		c.status[reply.Id] = in_progress
		reply.Identity = reduce_task
	}

	return nil
}

func (c *Coordinator) Worker_is_completed(input *Reply, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// println(input.Id, input.Identity)
	c.status[input.Id] = completed
	c.count_of_completed_task++
	if input.Id < c.nMap {
		c.count_of_completed_map_task++
		if c.count_of_completed_map_task == c.nMap {
			c.map_task_done = true
		}
	}

	if c.count_of_completed_task == c.nMap+c.nReduce {
		// println(5555)
		c.done = true
	}

	if c.count_of_completed_map_task == c.nMap {
		cv.Signal()
	}
	return nil
}

func (c *Coordinator) All_map_tasks_finish(args *Status, reply *Status) error {
	// for i := 0; i < c.nMap; i++ {
	// 	if c.status[i] != completed {
	// 		reply.Status = false
	// 		return nil
	// 	}
	// }
	reply.Status = c.map_task_done
	// cv.Signal() //need testing
	return nil
}

func (c *Coordinator) All_tasks_finish(args *Status, reply *Status) error {
	reply.Status = c.done
	return nil
}

func (c *Coordinator) watch() {
	for !c.done {
		time.Sleep(time.Second)
		c.mutex.Lock()
		for i := 0; i < c.nMap+c.nReduce; i++ {
			if c.status[i] == in_progress && c.start_time[i].Add(time.Second*10).Before(time.Now()) {
				// println(i, time.Now().Sub(c.start_time[i]).Seconds())
				c.status[i] = idle
				if i < c.nMap {
					c.map_channel <- i
				} else {
					c.reduce_channel <- i
				}
			}
			// if c.status[i] == in_progress {
			// 	println(i, time.Now().Sub(c.start_time[i]).Seconds())
			// }
		}
		c.mutex.Unlock()
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
	time.Sleep(time.Second)
	// c.cv.L.Lock()
	// for !c.done {
	// 	c.cv.Wait()
	// }
	// c.cv.L.Unlock()
	// Your code here.

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)

	c := Coordinator{status: make([]int, nMap+nReduce), identity: make([]int, nMap+nReduce)}
	// Your code here.

	c.start_time = make([]time.Time, nMap+nReduce)
	c.nMap = nMap
	c.nReduce = nReduce
	c.files = files
	// c.cv = *sync.NewCond(&sync.Mutex{})
	c.map_channel = make(chan int, nMap)
	c.reduce_channel = make(chan int, nReduce)
	c.done = false
	for i := 0; i < nMap; i++ {
		c.map_channel <- i
	}
	for i := 0; i < nReduce; i++ {
		c.reduce_channel <- i + nMap
	}
	go c.watch()
	c.server()
	return &c
}
