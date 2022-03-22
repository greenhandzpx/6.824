package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type FileInfo struct {
	// 存储文件的状态
	// 0: 未完成；1: 正在执行; 2: 已完成
	State int
	// 表示文件的次序
	Order int
	// 表示文件刚被分配出去时的时间点
	Starttime int64
}
type Coordinator struct {
	// Your definitions here.
	Files       []string
	States      map[string]FileInfo
	NumFinished int

	NumReduce            int
	StatesForReduce      []FileInfo
	NumFinishedForReduce int

	Mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AllocTask(args *AskForTask_Args, reply *AskForMapTask_Reply) error {
	c.Mutex.Lock()

	// 这里统一先设成false
	reply.AllFinished = false

	if c.NumFinished == len(c.Files) &&
		c.NumFinishedForReduce == c.NumReduce {
		// 两种任务都已完成，则可通知worker退出循环
		reply.AllFinished = true
		c.Mutex.Unlock()
		return nil
	}

	if c.NumFinished == len(c.Files) {
		// 此时所有map任务都已完成，进行reduce任务
		for i, info := range c.StatesForReduce {
			if info.State == 0 {
				// 找到一个未开始的reduce任务
				reply.TaskType = 2
				reply.ReduceOrder = i
				reply.NumMap = len(c.Files)
				c.StatesForReduce[i] = FileInfo{1, i, time.Now().Unix()}
				c.Mutex.Unlock()
				return nil
			} else if info.State == 1 {
				if time.Now().Unix()-info.Starttime >= 10 {
					// 该reduce任务已经超时，可以重新分配
					reply.TaskType = 2
					reply.ReduceOrder = i
					reply.NumMap = len(c.Files)

					c.StatesForReduce[i] = FileInfo{1, i, time.Now().Unix()}
					c.Mutex.Unlock()
					return nil
				}
			}
		}
		reply.TaskType = 3
		c.Mutex.Unlock()
		return nil
	}

	// 进行map任务
	for file, info := range c.States {
		if info.State == 0 {
			// 找到一个未完成的任务
			//fmt.Println("map task: ", info.Order)
			reply.TaskType = 1
			reply.File = file
			reply.NumReduce = c.NumReduce
			reply.FileOrder = info.Order
			// 1表示正在执行
			order := c.States[file].Order
			c.States[file] = FileInfo{1, order, time.Now().Unix()}

			c.Mutex.Unlock()
			return nil

		} else if info.State == 1 {
			if time.Now().Unix()-info.Starttime >= 10 {
				// 大于10秒说明超时了，可以重新分配
				//fmt.Println("map task: ", info.Order)
				reply.TaskType = 1
				reply.File = file
				reply.NumReduce = c.NumReduce
				reply.FileOrder = info.Order
				// 1表示正在执行
				order := c.States[file].Order
				c.States[file] = FileInfo{1, order, time.Now().Unix()}

				c.Mutex.Unlock()
				return nil
			}
		}
	}
	// 表示没有任务
	reply.TaskType = 3
	c.Mutex.Unlock()
	return nil
}

// 接收worker完成Map任务的消息
func (c *Coordinator) ReceiveFinished(args *InformFinished_Args, reply *InformFinished_Reply) error {
	// 直接将状态改成2（表示已完成），其他随便填
	c.Mutex.Lock()
	if c.States[args.File].State == 2 {
		// 说明已经完成了，直接忽略
		c.Mutex.Unlock()
		return nil
	}

	//fmt.Println("numfinished: ", c.NumFinished)
	//fmt.Println("numfinishedorder: ", c.States[args.File].Order)
	c.States[args.File] = FileInfo{2, -1, 0}
	c.NumFinished++
	c.Mutex.Unlock()
	return nil
}

// 接收worker完成reduce任务的消息
func (c *Coordinator) ReceiveFinishedForReduce(args *InformFinished_Args, reply *InformFinished_Reply) error {
	// 直接将状态改成2（表示已完成），其他随便填
	c.Mutex.Lock()
	if c.StatesForReduce[args.ReduceOrder].State == 2 {
		// 该reduce任务之前已被完成，直接忽略
		c.Mutex.Unlock()
		return nil
	}
	c.StatesForReduce[args.ReduceOrder] = FileInfo{2, -1, 0}
	c.NumFinishedForReduce++
	c.Mutex.Unlock()
	return nil
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
	ret := false

	// Your code here.
	c.Mutex.Lock()
	if c.NumFinished == len(c.Files) &&
		c.NumFinishedForReduce == c.NumReduce {
		ret = true
	}
	c.Mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("file size: ", len(files))
	c := Coordinator{
		Files:                files,
		NumFinished:          0,
		NumReduce:            nReduce,
		NumFinishedForReduce: 0}
	c.States = make(map[string]FileInfo)
	for i, file := range files {
		// 0表示还没处理
		fmt.Println("FileOrder: ", i)
		fmt.Println("Filename:", file)
		c.States[file] = FileInfo{0, i, 0}
	}
	c.StatesForReduce = make([]FileInfo, nReduce)
	for i := 0; i < nReduce; i++ {
		// 0表示还没处理
		c.StatesForReduce[i] = FileInfo{0, i, 0}
	}
	//fmt.Println(c.States[files[0]].Order, c.States[files[0]].State)
	//fmt.Println("File count: ", len(c.Files))
	c.server()
	return &c
}
