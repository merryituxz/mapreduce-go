package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func FetchTask() Task {
	args := ExampleArgs{}
	task := Task{}
	call("Coordinator.AllocateTask", &args, &task)
	// if task.Type == Map {
	// 	fmt.Printf("fetch map task[%d]\n", task.Id)
	// } else if task.Type == Reduce {
	// 	fmt.Printf("fetch reduce task[%d]\n", task.Id)
	// }
	return task
}

func writeToLocalFile(x, y int, kva *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("cannot create tempfile", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("failed to write kv pair", err)
		}
	}
	tempFile.Close()
	fileName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), fileName)
	return filepath.Join(dir, fileName)
}

func doMap(task *Task, mapf func(string, string) []KeyValue) {
	nReduce := task.NReduce
	intermediate := make([][]KeyValue, nReduce)

	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()

	kva := mapf(task.FileName, string(content))

	// 将产生的kv hash到对应的位置
	for _, kv := range kva {
		index := ihash(kv.Key) % nReduce
		intermediate[index] = append(intermediate[index], kv)
	}

	var fileNames []string
	for i := 0; i < nReduce; i++ {
		fileNames = append(fileNames, writeToLocalFile(task.Id, i, &intermediate[i]))
	}

	result := Result{
		Id:            task.Id,
		Type:          Map,
		Intermediates: fileNames,
	}
	// 处理完后将R个文件的地址通过RPC传给Coordinator
	call("Coordinator.TaskComplete", &result, &ExampleReply{})
}

func doReduce(task *Task, reducef func(string, []string) string) {
	var kva []KeyValue
	for _, fileName := range task.Intermediates {
		f, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("can not open file %s\n", fileName)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(kva))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("cannot create tempfile", err)
	}

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
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tempFile.Name(), oname)

	result := Result{
		Id:   task.Id,
		Type: Reduce,
	}
	// 处理完后将R个文件的地址通过RPC传给Coordinator
	call("Coordinator.TaskComplete", &result, &ExampleReply{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		task := FetchTask()

		switch task.Type {
		case Map:
			doMap(&task, mapf)
		case Reduce:
			doReduce(&task, reducef)
		case Wait:
			time.Sleep(time.Second * 3)
		case Exit:
			return
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
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
