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
	Idle = iota
	InProgress
	Completed
)

type TaskMeta struct {
	TaskId        int
	TaskStatus    int // 当前Task所处的状态[Idle/InProgress/Completed]
	TaskReference *Task
	StartTime     time.Time
}

type Coordinator struct {
	NMap          int // Map Task的数量
	NReduce       int // Reduce Task的数量
	State         int // 当前Coordinator所处的状态[Map/Reduce/Exit]
	TaskMeta      map[int]*TaskMeta
	TaskQueue     chan *Task // 待分发的Task
	Files         []string
	Intermediates [][]string // Map过程生成的中间文件
	sync.Mutex
}

func (c *Coordinator) AllocateTask(req *Request, resp *Task) error {
	c.Lock()
	defer c.Unlock()

	if c.State == Exit {
		*resp = Task{
			Type: Exit,
		}
	}

	if len(c.TaskQueue) > 0 {
		*resp = *<-c.TaskQueue
		c.TaskMeta[resp.Id].TaskStatus = InProgress
		c.TaskMeta[resp.Id].StartTime = time.Now()
	} else {
		*resp = Task{
			Type: Wait,
		}
	}
	return nil
}

func (c *Coordinator) catchTimeout() {
	for {
		c.Lock()
		if c.State == Exit {
			c.Unlock()
			return
		}
		for _, taskMeta := range c.TaskMeta {
			// 超时任务重新放入队列中
			if taskMeta.TaskStatus == InProgress && time.Since(taskMeta.StartTime) >= 10*time.Second {
				taskMeta.TaskStatus = Idle
				c.TaskQueue <- taskMeta.TaskReference
			}
		}
		c.Unlock()
	}
}

func (c *Coordinator) TaskComplete(result *Result, resp *ExampleReply) error {
	c.Lock()
	defer c.Unlock()

	// 收到已完成的任务
	if c.TaskMeta[result.Id].TaskStatus == Completed {
		return nil
	}

	// 收到过期任务
	if c.State != result.Type {
		return nil
	}

	c.TaskMeta[result.Id].TaskStatus = Completed

	switch result.Type {
	case Map:
		for i := 0; i < c.NReduce; i++ {
			c.Intermediates[i] = append(c.Intermediates[i], result.Intermediates[i])
		}
	case Reduce:

	}

	if c.AllTaskCompleted() {
		switch c.State {
		case Map:
			c.State = Reduce
			c.TaskMeta = make(map[int]*TaskMeta)
			for i, intermediate := range c.Intermediates {
				task := &Task{
					Id:            i,
					Type:          Reduce,
					NMap:          c.NMap,
					NReduce:       c.NReduce,
					Intermediates: intermediate,
				}
				taskMeta := &TaskMeta{
					TaskId:        i,
					TaskStatus:    Idle,
					TaskReference: task,
					StartTime:     time.Time{},
				}
				c.TaskQueue <- task
				c.TaskMeta[task.Id] = taskMeta
			}
		case Reduce:
			c.State = Exit
		}
	}

	return nil
}

// 判断当前Task是否已全部完成(Completed)
func (c *Coordinator) AllTaskCompleted() bool {
	for _, t := range c.TaskMeta {
		if t.TaskStatus != Completed {
			return false
		}
	}
	return true
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()

	return c.State == Exit
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		NMap:          len(files),
		NReduce:       nReduce,
		State:         Map,
		TaskMeta:      make(map[int]*TaskMeta),
		TaskQueue:     make(chan *Task, max(len(files), nReduce)),
		Files:         files,
		Intermediates: make([][]string, nReduce),
	}

	for i, file := range files {
		task := &Task{
			Id:       i,
			Type:     Map,
			NMap:     c.NMap,
			NReduce:  c.NReduce,
			FileName: file,
		}
		taskMeta := &TaskMeta{
			TaskId:        i,
			TaskStatus:    Idle,
			TaskReference: task,
			StartTime:     time.Time{},
		}
		c.TaskQueue <- task
		c.TaskMeta[task.Id] = taskMeta
	}

	go c.catchTimeout()
	c.server()
	return &c
}
