package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Map functions return a slice of KeyValue.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

type OutputKV struct {
	Key   string
	Value int
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
	for i := 0; i < 1; i -= 1 {
		Task := Call2()
		// fmt.Println(Task)
		if Task.Ttype == "M" {
			Temp(Task.MA, Task.Reducenum, mapf)
		} else if Task.Ttype == "R" {
			Readtemp(Task.RA, reducef)
		} else if Task.Ttype == "F" {
			break
		} else {
			continue
		}
		Call3(Task.TID)
	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Call1() {

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

func Temp(M MapAssignment, Rn int, mapf func(string, string) []KeyValue) {
	X := M.Id
	filename := M.Filename
	file, err := os.Open(filename)
	// fmt.Println(M)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// this is the correct format for each line of Reduce output.
		file, err := os.OpenFile(fmt.Sprintf("mr-%d-%d", X, ihash(kva[i].Key)%Rn), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
		}
		fmt.Fprintf(file, "%v,%v\n", kva[i].Key, values)
		i = j
	}
}

func Readtemp(Rindex int, reducef func(string, []string) string) {
	dir := "."
	files, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	dict := make(map[string][]string)
	regex := regexp.MustCompile(fmt.Sprintf(`^mr-\d+-%d$`, Rindex))
	for _, file := range files {
		if regex.MatchString(file.Name()) {
			file, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("cannot read %v", file)
			}
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				T := strings.Split(scanner.Text(), ",")
				key := T[0]
				values := T[1]
				list := strings.Fields(values[1 : len(values)-1])
				dict[key] = append(dict[key], list...)
			}
		}
	}
	if len(dict) == 0 {
		return
	}
	oname := fmt.Sprintf("mr-out-%d", Rindex)
	ofile, _ := os.Create(oname)
	for k, v := range dict {
		output := reducef(k, v)
		fmt.Fprintf(ofile, "%v %v\n", k, output)
	}
}

func Call2() Task {
	// declare an argument structure.
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.Idle", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func Call3(TID int) {
	// declare an argument structure.
	args := TID
	// fill in the argument(s).
	// declare a reply structure.
	reply := ExampleReply{}
	ok := call("Coordinator.Check", &args, &reply)
	if !ok {
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
