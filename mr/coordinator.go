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

type Queue[T any] struct {
	elements []T
}

type MapAssignment struct {
	Id       int
	Filename string
}

type Coordinator struct {
	// Your definitions here.
	TaskID    int
	Reducenum int
	MAlist    Queue[MapAssignment]
	MAcheck   []int
	MAfinish  bool
	RAlist    Queue[int]
	RAcheck   []int
	RAfinish  bool
	TaskList  map[int]int64
	Taskmap   map[int]Task
}

var mu sync.RWMutex

func (q *Queue[T]) Pop() (T, bool) {
	var zeroV T
	if len(q.elements) == 0 {
		return zeroV, false
	}
	v := q.elements[0]
	q.elements = q.elements[1:]
	return v, true
}

func deleteFromML(m map[int]int64, mu *sync.RWMutex, key int) {
	mu.Lock()
	defer mu.Unlock()
	delete(m, key)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Check(args int, reply *ExampleReply) error {
	_, exists := c.TaskList[args]
	A := c.Taskmap[args]
	if !exists {
		fmt.Printf("Task %d finish but too late.\n", A.TID)
	}
	if A.Ttype == "M" {
		c.MAcheck[A.MA.Id] = 1
		s := 0
		for i := 0; i < len(c.MAcheck); i++ {
			s += c.MAcheck[i]
		}
		if s == len(c.MAcheck) {
			c.MAfinish = true
		}
	} else if A.Ttype == "R" {
		c.RAcheck[A.RA] = 1
		s := 0
		for i := 0; i < len(c.RAcheck); i++ {
			s += c.RAcheck[i]
		}
		if s == len(c.RAcheck) {
			c.RAfinish = true
		}
	}
	deleteFromML(c.TaskList, &mu, args)
	return nil
}

func TaskListupd(c *Coordinator) error {
	curtime := time.Now().Unix()
	for ki, v := range c.TaskList {
		// fmt.Println(c.TaskList)
		k := c.Taskmap[ki]
		if v < curtime-10 {
			if k.Ttype == "M" {
				c.MAlist.elements = append(c.MAlist.elements, k.MA)
			} else if k.Ttype == "R" {
				c.RAlist.elements = append(c.RAlist.elements, k.RA)
			}
			deleteFromML(c.TaskList, &mu, ki)
		}

	}
	return nil
}

func (c *Coordinator) Idle(args *ExampleArgs, reply *Task) error {
	reply.TID = c.TaskID
	mu.Lock()
	c.TaskList[c.TaskID] = time.Now().Unix()
	mu.Unlock()
	// fmt.Println(c.MAlist, c.RAlist, c.TaskList)
	if !c.MAfinish && len(c.MAlist.elements) != 0 {
		Tsk, _ := c.MAlist.Pop()
		reply.MA = Tsk
		reply.Reducenum = c.Reducenum
		reply.Ttype = "M"
	} else if c.MAfinish && !c.RAfinish && len(c.RAlist.elements) != 0 {
		Tsk, _ := c.RAlist.Pop()
		reply.RA = Tsk
		reply.Ttype = "R"
	} else if c.RAfinish {
		reply.Ttype = "F"
		return nil
	} else {
		return nil
	}
	mu.Lock()
	c.Taskmap[c.TaskID] = *reply
	mu.Unlock()
	c.TaskID += 1
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
	TaskListupd(c)
	// Your code here.

	return c.RAfinish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskID:    0,
		Reducenum: nReduce,
		MAfinish:  false,
		RAfinish:  false,
		TaskList:  make(map[int]int64),
		Taskmap:   make(map[int]Task),
	}
	index := 0
	for i := 0; i < nReduce; i++ {
		c.RAlist.elements = append(c.RAlist.elements, i)
		c.RAcheck = append(c.RAcheck, 0)
	}
	for _, filename := range os.Args[1:] {
		newma := MapAssignment{
			Id:       index,
			Filename: filename,
		}
		index += 1
		c.MAlist.elements = append(c.MAlist.elements, newma)
		c.MAcheck = append(c.MAcheck, 0)
	}
	// Your code here.
	c.server()
	return &c
}
